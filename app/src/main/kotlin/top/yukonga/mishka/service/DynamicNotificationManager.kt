package top.yukonga.mishka.service

import android.app.NotificationManager
import android.content.Context
import android.os.SystemClock
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.launch
import top.yukonga.mishka.R
import top.yukonga.mishka.data.api.MihomoConnectionManager
import top.yukonga.mishka.platform.PlatformStorage
import top.yukonga.mishka.platform.StorageKeys
import top.yukonga.mishka.platform.TunMode
import top.yukonga.mishka.util.FormatUtils

/**
 * 动态通知管理器。
 * 三种模式统一从共享的 [MihomoConnectionManager.repository] 收集 `/traffic` WS，
 * 通知侧节流到 5s 更新一次。
 * 不持有自己的 HttpClient——所有 mihomo 客户端实例由 connectionManager 单点管理。
 */
class DynamicNotificationManager(
    private val context: Context,
    private val scope: CoroutineScope,
    private val connectionManager: MihomoConnectionManager,
) {

    // start() 来自服务启动协程与 notificationRefresh 收集器（不同 IO 线程），stop() 可能来自 onDestroy
    @Volatile
    private var trafficJob: Job? = null

    @Volatile
    private var lastNotifyAt = 0L

    @Volatile
    private var lastTitle: String? = null

    @Volatile
    private var lastContent: String? = null

    @OptIn(ExperimentalCoroutinesApi::class)
    @Synchronized
    private fun start(profileName: String, mode: String) {
        val notificationManager = context.getSystemService(NotificationManager::class.java)

        trafficJob?.cancel()
        lastNotifyAt = 0L
        lastTitle = null
        lastContent = null

        // mihomo 重启时 connectionManager.repository 切到新实例，flatMapLatest 自动取消旧 trafficFlow 收集
        trafficJob = scope.launch {
            connectionManager.repository
                .filterNotNull()
                .flatMapLatest { it.trafficFlow() }
                .collect { traffic ->
                    val now = SystemClock.elapsedRealtime()
                    if (lastNotifyAt != 0L && now - lastNotifyAt < NOTIFY_INTERVAL_MS) return@collect
                    lastNotifyAt = now
                    val title = "$profileName • " +
                            "${FormatUtils.formatBytes(traffic.upTotal)}↑ ${FormatUtils.formatBytes(traffic.downTotal)}↓"
                    val content = "${FormatUtils.formatSpeed(traffic.up)}↑ " +
                            "${FormatUtils.formatSpeed(traffic.down)}↓ · $mode"
                    if (title == lastTitle && content == lastContent) return@collect
                    lastTitle = title
                    lastContent = content
                    runCatching {
                        val notification = NotificationHelper.buildDynamicNotification(
                            context = context,
                            title = title,
                            content = content,
                        )
                        notificationManager?.notify(NotificationHelper.NOTIFICATION_ID_VPN, notification)
                    }.onFailure { Log.w(TAG, "Notify failed: $it") }
                }
        }
    }

    /**
     * 根据设置启动动态通知，并先发一条静态运行通知兜底。
     *
     * VPN / ROOT TUN / ROOT TPROXY 共用同一条 `/traffic` WS：`up`/`down` 与
     * `upTotal`/`downTotal` 都是 mihomo 的代理口径，ROOT 通知的速率语义与主页一致，
     * 不另采设备级计数器。内容最多每 [NOTIFY_INTERVAL_MS] 重建一次，内容不变则跳过 notify。
     * 前台服务仍是现有的 specialUse FGS，不申请 wake lock 或电池优化豁免——深度 Doze 下
     * 系统仍会冻结定时器与 WS，刷新会暂停到亮屏/退出 Doze，这是平台限制而非缺通知路径。
     *
     * 静态 Running 通知必须先发：WS 首帧可能迟到或不可用，否则前台通知会停在
     * [NotificationHelper.buildLoadingNotification] 的「启动中」。
     */
    fun startOrFallbackStatic(storage: PlatformStorage, tunMode: TunMode = TunMode.Vpn) {
        // 与设置页的隧道模式名同源，避免通知里出现「Root TUN」而设置里写「ROOT TUN」
        val mode = context.getString(
            when (tunMode) {
                TunMode.RootTun -> R.string.settings_tun_mode_root_tun
                TunMode.RootTproxy -> R.string.settings_tun_mode_root_tproxy
                TunMode.Vpn -> R.string.settings_tun_mode_vpn
            }
        )
        runCatching {
            val notification = NotificationHelper.buildRunningNotification(context, mode)
            context.getSystemService(NotificationManager::class.java)
                ?.notify(NotificationHelper.NOTIFICATION_ID_VPN, notification)
        }.onFailure { Log.w(TAG, "Static notify failed: $it") }

        val isDynamic = storage.getString(StorageKeys.DYNAMIC_NOTIFICATION, "true") != "false"
        if (!isDynamic) return
        val profileName = storage.getString(StorageKeys.ACTIVE_PROFILE_NAME, "Mishka")
        start(profileName, mode)
    }

    @Synchronized
    fun stop() {
        trafficJob?.cancel()
        trafficJob = null
        lastNotifyAt = 0L
        lastTitle = null
        lastContent = null
    }

    companion object {
        private const val TAG = "DynamicNotification"

        // 1 Hz 的 /traffic WS 帧与 notify() 在后台会被系统批量合并，逐帧重建通知没有收益；
        // 5s 与前台服务通知的刷新节奏一致，也避开通知栏高频重绘
        private const val NOTIFY_INTERVAL_MS = 5_000L
    }
}
