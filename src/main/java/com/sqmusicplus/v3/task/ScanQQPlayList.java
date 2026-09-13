package com.sqmusicplus.v3.task;

import cn.hutool.crypto.digest.DigestUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.sqmusicplus.v3.base.entity.DownloadInfo;
import com.sqmusicplus.v3.base.entity.SqSync;
import com.sqmusicplus.v3.base.enums.DbBooleanConvert;
import com.sqmusicplus.v3.base.enums.PlugBrType;
import com.sqmusicplus.v3.base.enums.SetConfigEnum;
import com.sqmusicplus.v3.base.service.DownloadInfoService;
import com.sqmusicplus.v3.base.service.SqSyncService;
import com.sqmusicplus.v3.config.SqConfigCache;
import com.sqmusicplus.v3.monitor.entity.SqMonitor;
import com.sqmusicplus.v3.monitor.enums.MonitorType;
import com.sqmusicplus.v3.monitor.service.SqMonitorService;
import com.sqmusicplus.v3.plug.entity.Music;
import com.sqmusicplus.v3.plug.qq.entity.DissInfo;
import com.sqmusicplus.v3.plug.qqvip.QQvipHander;
import com.sqmusicplus.v3.utils.StringUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @Classname ScanQQPlayList
 * @Description 扫描QQ音乐歌单（增量同步版本）
 * 匿名 CgiGetDiss 拉取公开歌单：首页仅取 dirinfo.mtime 做变更检测（稳态每歌单每分钟1请求），
 * mtime 变化才分页拉全量；下载依赖 QQ 登录态（PLUG_QQVIP_COOKIE）。
 * @Version 1.0.0
 * @Date 2026/9/13
 * @Created by SQ
 */
@Slf4j
@Component
public class ScanQQPlayList {

    private static final String QQ_DIRID = "1418";
    /**
     * songListInfoRequestParam 固定每页50条
     */
    private static final int PAGE_SIZE = 50;
    /**
     * 分页拉取防御性上限（40*50=2000首）
     */
    private static final int MAX_PAGES = 40;

    @Autowired
    private QQvipHander qqvipHander;

    @Autowired
    private SqMonitorService monitorService;

    @Autowired
    private DownloadInfoService downloadInfoService;

    @Autowired
    private SqSyncService syncService;

    /**
     * 防止定时任务并发执行导致重复插入
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    @PostConstruct
    public void debug() {
        log.debug("ScanQQPlayList QQ音乐歌单扫描已注册, cron=20 */1 * * * ? (每1分钟)");
    }

    @Scheduled(cron = "20 */1 * * * ? ")
    public void excute() {
        // 防止上一次执行未完成时重复执行（@Scheduled + synchronized 在代理下不可靠）
        if (!running.compareAndSet(false, true)) {
            log.warn("上一次QQ音乐歌单扫描尚未完成，跳过本次执行");
            return;
        }
        try {
            String qqopen = SqConfigCache.getSqConfigValue(SetConfigEnum.PLUG_QQVIP_OPEN);
            if (StringUtils.isNotBlank(qqopen)) {
                log.info("开始扫描QQ音乐歌单");
                queryAndDownloadPlayList();
            }
        } catch (Throwable t) {
            log.error("QQ音乐歌单扫描定时任务执行异常，等待重试！", t);
        } finally {
            running.set(false);
        }
    }

    private void queryAndDownloadPlayList() {
        ArrayList<String> excludeArtistNames = new ArrayList<>();
        ArrayList<String> excludeAlbumNames = new ArrayList<>();
        LambdaQueryWrapper<SqMonitor> sqMonitorLambdaQueryWrapper = new LambdaQueryWrapper<SqMonitor>()
                .eq(SqMonitor::getPlugName, qqvipHander.getPlugName())
                .eq(SqMonitor::getEnabled, DbBooleanConvert.YES.getValue().intValue())
                .eq(SqMonitor::getType, MonitorType.PLAYLIST.getCode());
        List<SqMonitor> list = monitorService.list(sqMonitorLambdaQueryWrapper);
        if (!list.isEmpty()) {
            String excludeAlbum = SqConfigCache.getSqConfigValue(SetConfigEnum.SYSTEM_SYNC_ALBUM_EXCLUDE);
            String excludeArtists = SqConfigCache.getSqConfigValue(SetConfigEnum.SYSTEM_SYNC_ARTISTS_EXCLUDE);
            if (StringUtils.isNotBlank(excludeArtists)) {
                String[] split = excludeArtists.split("\\|");
                if (split != null) {
                    for (String s : split) {
                        excludeArtistNames.add(s.trim());
                    }
                }
            }
            if (StringUtils.isNotBlank(excludeAlbum)) {
                String[] split = excludeAlbum.split("\\|");
                if (split != null) {
                    for (String s : split) {
                        excludeAlbumNames.add(s.trim());
                    }
                }
            }

            for (SqMonitor sqMonitor : list) {
                String targetId = sqMonitor.getTargetId();
                try {
                    processPlaylistIncremental(sqMonitor, targetId, excludeArtistNames, excludeAlbumNames);
                } catch (Exception e) {
                    log.error("监听QQ音乐歌单信息失败: targetId=" + targetId, e);
                }
            }
        }
    }

    /**
     * 增量同步QQ音乐歌单
     * 1. 首页请求取 dirinfo.mtime 与 targetUpdateTime 对比，未变化直接跳过（稳态每分钟每歌单1请求）
     * 2. 变化才分页拉全量，与数据库已有记录对比找出新增歌曲
     */
    private void processPlaylistIncremental(SqMonitor sqMonitor, String targetId,
                                             ArrayList<String> excludeArtistNames,
                                             ArrayList<String> excludeAlbumNames) {
        // 第一步：首页请求（1次），取 dirinfo 做变更检测
        DissInfo dissInfo = qqvipHander.songListInfo(targetId, QQ_DIRID, 1L);
        if (dissInfo == null || dissInfo.getCode() == null || dissInfo.getCode() != 0L
                || dissInfo.getData() == null) {
            log.warn("QQ音乐歌单首页拉取失败，跳过: targetId={}, name={}", targetId, sqMonitor.getTargetName());
            return;
        }
        DissInfo.DataDTO data = dissInfo.getData();
        DissInfo.DataDTO.DirinfoDTO dirinfo = data.getDirinfo();
        if (dirinfo == null) {
            log.warn("QQ音乐歌单无 dirinfo（可能非公开歌单），跳过: targetId={}", targetId);
            return;
        }
        Long mtime = dirinfo.getMtime();
        List<DissInfo.DataDTO.SonglistDTO> firstPageSonglist = data.getSonglist();

        // 变更检测：mtime 未变化且已有同步记录则跳过全量拉取
        if (sqMonitor.getTargetUpdateTime() != null && mtime != null
                && sqMonitor.getTargetUpdateTime().equals(mtime)) {
            return;
        }

        // 第二步：更新歌单元数据
        sqMonitor.setUpdateTime(new Date());
        if (dirinfo.getSongnum() != null) {
            sqMonitor.setTargetCount(dirinfo.getSongnum());
        }
        sqMonitor.setTargetUpdateTime(mtime);
        if (StringUtils.isNotBlank(dirinfo.getTitle())) {
            sqMonitor.setTargetName(dirinfo.getTitle());
        }
        if (StringUtils.isNotBlank(dirinfo.getPicurl())) {
            sqMonitor.setTargetCover(dirinfo.getPicurl());
        }
        if (StringUtils.isNotBlank(dirinfo.getDesc())) {
            sqMonitor.setTargetDesc(dirinfo.getDesc());
        }
        monitorService.updateById(sqMonitor);

        if (CollectionUtils.isEmpty(firstPageSonglist)) {
            log.info("QQ音乐歌单无歌曲: targetId={}, name={}", targetId, sqMonitor.getTargetName());
            return;
        }

        // 第三步：分页拉全量（首页已含第1页，songnum 缺失时以首页为准）
        List<DissInfo.DataDTO.SonglistDTO> allSongs = new ArrayList<>(firstPageSonglist);
        long songnum = dirinfo.getSongnum() != null ? dirinfo.getSongnum() : allSongs.size();
        long expectPages = (songnum + PAGE_SIZE - 1) / PAGE_SIZE;
        int totalPages = (int) Math.min(expectPages, MAX_PAGES);
        for (int p = 2; p <= totalPages; p++) {
            DissInfo pageDiss = qqvipHander.songListInfo(targetId, QQ_DIRID, (long) p);
            if (pageDiss == null || pageDiss.getCode() == null || pageDiss.getCode() != 0L
                    || pageDiss.getData() == null
                    || CollectionUtils.isEmpty(pageDiss.getData().getSonglist())) {
                log.warn("QQ音乐歌单第{}页拉取失败或为空，提前结束分页: targetId={}", p, targetId);
                break;
            }
            allSongs.addAll(pageDiss.getData().getSonglist());
        }
        // 按 mid 去重（防御分页边界重复）
        Set<String> seen = new HashSet<>();
        allSongs.removeIf(item -> item == null || StringUtils.isBlank(item.getMid()) || !seen.add(item.getMid()));

        // 第四步：获取数据库中已同步的歌曲ID
        LambdaQueryWrapper<SqSync> sqSyncQuery = new LambdaQueryWrapper<SqSync>()
                .eq(SqSync::getPlugName, qqvipHander.getPlugName())
                .eq(SqSync::getPlayListId, targetId);
        List<SqSync> dbSqSync = syncService.list(sqSyncQuery);
        Set<String> downloadedMusicIds = dbSqSync.stream()
                .map(SqSync::getMusicId)
                .collect(Collectors.toSet());

        // 第五步：转换并过滤出新增歌曲
        ArrayList<SqSync> sqSyncs = new ArrayList<>();
        ArrayList<DownloadInfo> downloadInfos = new ArrayList<>();
        int newCount = 0;
        for (DissInfo.DataDTO.SonglistDTO item : allSongs) {
            if (downloadedMusicIds.contains(item.getMid())) {
                continue;
            }
            Music music = convertSong(item);
            if (music == null) {
                continue;
            }
            if (StringUtils.isNotBlank(music.getMusicAlbum()) && excludeAlbumNames.contains(music.getMusicAlbum())) {
                continue;
            }
            boolean needExclude = checkNeedExclude(excludeArtistNames, music.getMusicArtists());
            if (needExclude) {
                continue;
            }
            DownloadInfo downloadInfo = qqvipHander.musicToDownloadInfo(music, null, false);
            if (downloadInfo == null) {
                log.warn("QQ音乐歌曲转下载信息失败，跳过: mid={}, name={}", item.getMid(), item.getTitle());
                continue;
            }
            downloadInfos.add(downloadInfo);
            SqSync sqSync = new SqSync();
            sqSync.setMusicId(music.getId());
            sqSync.setPlugName(qqvipHander.getPlugName());
            sqSync.setMusicInfo(JSON.toJSONString(music));
            sqSync.setPlayListName(sqMonitor.getTargetName());
            sqSync.setPlayListId(targetId);
            sqSync.setDownloadId(downloadInfo.getId());
            String playListSha1 = DigestUtil.sha1Hex(sqMonitor.getTargetName());
            sqSync.setPlayListSha1(playListSha1);
            sqSyncs.add(sqSync);
            newCount++;
        }

        if (downloadInfos.isEmpty()) {
            log.info("QQ音乐歌单无新增歌曲: targetId={}, name={}, 已同步{}首",
                    targetId, sqMonitor.getTargetName(), downloadedMusicIds.size());
            return;
        }

        log.info("QQ音乐歌单发现{}首新增歌曲: targetId={}, name={}", newCount, targetId, sqMonitor.getTargetName());

        // 第六步：入下载队列 + 保存同步记录（批量失败逐条兜底，防唯一约束冲突）
        downloadInfoService.add(downloadInfos);
        Set<String> existingIds = syncService.lambdaQuery()
                .eq(SqSync::getPlugName, qqvipHander.getPlugName())
                .eq(SqSync::getPlayListId, targetId)
                .list()
                .stream()
                .map(SqSync::getMusicId)
                .collect(Collectors.toSet());
        List<SqSync> newSyncs = sqSyncs.stream()
                .filter(s -> !existingIds.contains(s.getMusicId()))
                .collect(Collectors.toList());
        if (!newSyncs.isEmpty()) {
            try {
                syncService.saveBatch(newSyncs);
            } catch (Exception e) {
                // 唯一约束冲突时逐条保存并忽略重复（兜底保护）
                log.warn("批量保存SqSync冲突，逐条尝试: {}", e.getMessage());
                for (SqSync sync : newSyncs) {
                    try {
                        syncService.save(sync);
                    } catch (Exception ignored) {
                        // 唯一约束冲突，跳过已存在的记录
                    }
                }
            }
            log.info("QQ音乐歌单增量同步完成: targetId={}, 新增{}首", targetId, downloadInfos.size());
        }
    }

    /**
     * 将 CgiGetDiss 的 songlist 条目转换为通用 Music
     * 匿名接口无 file 字段，固定提供 128/320 两档（实际下载需 QQ 登录态，走 qqvip GetVkey）
     */
    private Music convertSong(DissInfo.DataDTO.SonglistDTO item) {
        try {
            Music music = new Music();
            music.setId(item.getMid());
            music.setMusicName(item.getTitle());
            if (CollectionUtils.isNotEmpty(item.getSinger())) {
                music.setMusicArtists(item.getSinger().stream()
                        .map(DissInfo.DataDTO.SonglistDTO.SingerDTO::getTitle)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList()));
                music.setArtistsIds(item.getSinger().stream()
                        .map(DissInfo.DataDTO.SonglistDTO.SingerDTO::getMid)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList()));
            }
            DissInfo.DataDTO.SonglistDTO.AlbumDTO album = item.getAlbum();
            if (album != null) {
                music.setMusicAlbum(album.getTitle());
                music.setAlbumId(album.getMid());
                if (StringUtils.isNotBlank(album.getPmid())) {
                    music.setMusicImage("https://y.qq.com/music/photo_new/T002R800x800M000" + album.getPmid() + ".jpg");
                }
            }
            if (item.getInterval() != null && item.getInterval() > 0) {
                music.setMusicDuration(item.getInterval() * 1000);
            }
            music.setPlugName(qqvipHander.getPlugName());
            music.setMusicFormat("mp3");
            // dataInfo 必须非空（musicToDownloadInfo 中会 toJSONString）
            music.setDataInfo((JSONObject) JSON.toJSON(item));
            // brType 传 null 由 getMaxBr 从 bits 中选最高档
            music.setBits(List.of(PlugBrType.QQVIP_MP3_128, PlugBrType.QQVIP_MP3_320));
            return music;
        } catch (Exception e) {
            log.warn("QQ音乐歌曲条目转换失败: mid={}", item != null ? item.getMid() : null, e);
            return null;
        }
    }

    /**
     * 判断当前歌手列表是否包含需要忽略的歌手，决定是否忽略当前歌曲
     * @param excludeArtistNames 需要忽略的歌手列表
     * @param musicArtists 当前歌曲的歌手列表
     * @return 包含则返回true（需要忽略），否则返回false
     */
    public static boolean checkNeedExclude(List<String> excludeArtistNames, List<String> musicArtists) {
        if (CollectionUtils.isEmpty(excludeArtistNames) || CollectionUtils.isEmpty(musicArtists)) {
            return false;
        }
        Set<String> musicArtistsSet = new HashSet<>(musicArtists);
        return excludeArtistNames.stream().anyMatch(musicArtistsSet::contains);
    }
}
