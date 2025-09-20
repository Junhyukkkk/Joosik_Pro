package com.joopro.Joosik_Pro.repository.viewcount;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joopro.Joosik_Pro.domain.Post.Post;
import com.joopro.Joosik_Pro.dto.PostDtoResponse;
import com.joopro.Joosik_Pro.repository.PostRepository;
import jakarta.annotation.PostConstruct;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisTopViewRepositoryImpl_Final implements TopViewRepositoryV2{
    private final PostRepository postRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String VIEW_DELTA_SET = "views:delta";
    private static final String POPULAR_POSTS_SET_KEY = "popularPostsZSet";
    private static final String LOCK_KEY = "topViewReadWriteLock";


    @Transactional
    @PostConstruct
    protected void init() {
        syncDbToRedis();
    }

    @Transactional
    @Override
    public void updateCacheWithDBAutomatically() {
        RReadWriteLock rwLock = redissonClient.getReadWriteLock(LOCK_KEY);
        boolean locked = false;
        try {
            locked = rwLock.writeLock().tryLock(5, 60, TimeUnit.SECONDS);
            if (locked) {
                updateViewCountsToDB();
                syncDbToRedis();
            } else {
                log.warn("updateCacheWithDBAutomatically writeLock 획득 실패");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("updateCacheWithDBAutomatically 중 인터럽트 발생", e);
        } finally {
            if (locked && rwLock.writeLock().isHeldByCurrentThread()) {
                rwLock.writeLock().unlock();
            }
        }
    }

    @Transactional
    @Override
    public PostDtoResponse returnPost(Long postId) {
        RReadWriteLock rwLock = redissonClient.getReadWriteLock(LOCK_KEY);
        PostDtoResponse postDtoResponse = null;
        boolean locked = false;
        try {
            locked = rwLock.readLock().tryLock(1, 2, TimeUnit.SECONDS);
            if (!locked) {
                log.warn("returnPost readLock 획득 실패");
                return null;
            }

            String redisKey = "post:" + postId;
            String postJson = redisTemplate.opsForValue().get(redisKey);

            if (postJson != null) {
                try {
                    redisTemplate.opsForZSet().incrementScore(POPULAR_POSTS_SET_KEY, String.valueOf(postId), 1.0);
                    postDtoResponse = objectMapper.readValue(postJson, PostDtoResponse.class);
                } catch (Exception e) {
                    log.warn("Redis에서 Post JSON 역직렬화 실패, DB 조회로 대체. postId={}", postId, e);
                    Post post = postRepository.findById(postId);
                    if (post == null) return null;

                    postDtoResponse = PostDtoResponse.of(post);
                    try {
                        String toJson = objectMapper.writeValueAsString(postDtoResponse);
                        redisTemplate.opsForValue().set(redisKey, toJson); // 재적재
                    } catch (JsonProcessingException ex) {
                        log.warn("DTO 직렬화 실패, 캐시 미적재. postId={}", postId, ex);
                    }
                }
            } else{
                Post post = postRepository.findById(postId);
                if (post == null) return null;

                postDtoResponse = PostDtoResponse.of(post);

//                try {
//                    String toJson = objectMapper.writeValueAsString(postDtoResponse);
//                    redisTemplate.opsForValue().set(redisKey, toJson); // 미스 하면 캐시 적재
//                } catch (JsonProcessingException ex) {
//                    log.warn("DTO 직렬화 실패, 캐시 미적재. postId={}", postId, ex);
//                }

                redisTemplate.opsForZSet().incrementScore(VIEW_DELTA_SET, String.valueOf(postId), 1.0); // 캐시에 없는 게시글 조회수 업데이트용 zset 증가
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("returnPost 중 인터럽트 발생", e);
            return null;
        } finally {
            if (locked && rwLock.readLock().isHeldByCurrentThread()) {
                rwLock.readLock().unlock();
            }
            return postDtoResponse;
        }
    }

    @Override
    public LinkedHashMap<Long, PostDtoResponse> getPopularPosts() {
        Set<String> postIdStrSet = redisTemplate.opsForZSet()
                .reverseRange(POPULAR_POSTS_SET_KEY, 0, 9);

        if (postIdStrSet == null || postIdStrSet.isEmpty()) {
            return new LinkedHashMap<>();
        }

        List<Long> postIds = postIdStrSet.stream()
                .map(Long::parseLong)
                .collect(Collectors.toList());

        // Redis에서 Post 정보 한 번에 꺼내오기
        LinkedHashMap<Long, PostDtoResponse> result = new LinkedHashMap<>();
        for (Long postId : postIds) {
            String redisKey = "post:" + postId;
            String postJson = redisTemplate.opsForValue().get(redisKey);
            PostDtoResponse postDtoResponse = null;
            if (postJson != null) {
                try {
                    postDtoResponse = objectMapper.readValue(postJson, PostDtoResponse.class);
                } catch (Exception e) {
                    log.warn("Redis에서 Post JSON 역직렬화 실패, DB 조회로 대체", e);
                }
            }
            // 없으면 DB에서 조회
            if (postDtoResponse == null) {
                postDtoResponse = PostDtoResponse.of(postRepository.findById(postId));
            }

            result.put(postId, postDtoResponse);

        }
        return result;
    }

    private void updateViewCountsToDB() {
        Set<String> allPostIds = redisTemplate.opsForZSet().range(POPULAR_POSTS_SET_KEY, 0, -1);
        if (allPostIds == null || allPostIds.isEmpty()) return;
        for (String idStr : allPostIds) {
            try {
                Long postId = Long.parseLong(idStr);
                Double score = redisTemplate.opsForZSet().score(POPULAR_POSTS_SET_KEY, idStr);
                if (score == null) continue;
                Post post = postRepository.findById(postId);
                if (post != null) {
                    post.setViewCount(score.longValue());
                    postRepository.save(post);
                }
            } catch (Exception e) {
                log.warn("ZSet 조회수 동기화 실패 - id: {}", idStr, e);
            }
        }
        redisTemplate.delete(POPULAR_POSTS_SET_KEY);

        Set<String> newPostIds = redisTemplate.opsForZSet().range(VIEW_DELTA_SET, 0, -1);
        if (newPostIds == null || newPostIds.isEmpty()) return;
        for (String idStr : newPostIds) {
            try {
                Long postId = Long.parseLong(idStr);
                Double score = redisTemplate.opsForZSet().score(VIEW_DELTA_SET, idStr);
                if (score == null) continue;
                Post post = postRepository.findById(postId);
                if (post != null) {
                    post.increaseViewCount(score.longValue());
                    postRepository.save(post);
                }
            } catch (Exception e) {
                log.warn("ZSet 조회수 동기화 실패 - id: {}", idStr, e);
            }
        }
        redisTemplate.delete(VIEW_DELTA_SET);
    }

    private void syncDbToRedis() {
        Set<String> keys = redisTemplate.keys("post:*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }

        List<Post> topPosts = postRepository.getPopularArticles();
        for (Post post : topPosts) {
            Long postId = post.getId();
            String redisKey = "post:" + postId;

            redisTemplate.opsForZSet().add(POPULAR_POSTS_SET_KEY,
                    String.valueOf(postId),
                    (double) post.getViewCount());

            try {
                PostDtoResponse dto = PostDtoResponse.of(post);
                String toJson = objectMapper.writeValueAsString(dto);

                redisTemplate.opsForValue().set("post:" + post.getId(), toJson);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
