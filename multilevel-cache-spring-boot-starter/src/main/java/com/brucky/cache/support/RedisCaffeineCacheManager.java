package com.brucky.cache.support;

import com.brucky.cache.prop.MultiLevelCacheProperties;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 一级缓存采用Caffeine，二级缓存采用Redis
 * @version 1.0.0
 */
public class RedisCaffeineCacheManager implements CacheManager {
	
	private final Logger logger = LoggerFactory.getLogger(RedisCaffeineCacheManager.class);
	
	private ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<String, Cache>();
	
	private MultiLevelCacheProperties multiLevelCacheProperties;
	
	private RedisTemplate<Object, Object> stringKeyRedisTemplate;

	private boolean dynamic = true;

	private Set<String> cacheNames;

	public RedisCaffeineCacheManager(MultiLevelCacheProperties multiLevelCacheProperties,
			RedisTemplate<Object, Object> stringKeyRedisTemplate) {
		super();
		this.multiLevelCacheProperties = multiLevelCacheProperties;
		this.stringKeyRedisTemplate = stringKeyRedisTemplate;
		this.dynamic = multiLevelCacheProperties.isDynamic();
		this.cacheNames = multiLevelCacheProperties.getCacheNames();
	}

	@Override
	public Cache getCache(String name) {
		Cache cache = cacheMap.get(name);
		if(cache != null) {
			return cache;
		}
		if(!dynamic && !cacheNames.contains(name)) {
			return cache;
		}
		
		cache = new RedisCaffeineCache(name, stringKeyRedisTemplate, caffeineCache(), multiLevelCacheProperties);
		Cache oldCache = cacheMap.putIfAbsent(name, cache);
		logger.debug("create cache instance, the cache name is : {}", name);
		return oldCache == null ? cache : oldCache;
	}
	
	public com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineCache(){
		Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
		if(multiLevelCacheProperties.getCaffeine().getExpireAfterAccess() > 0) {
			cacheBuilder.expireAfterAccess(multiLevelCacheProperties.getCaffeine().getExpireAfterAccess(), TimeUnit.MILLISECONDS);
		}
		if(multiLevelCacheProperties.getCaffeine().getExpireAfterWrite() > 0) {
			cacheBuilder.expireAfterWrite(multiLevelCacheProperties.getCaffeine().getExpireAfterWrite(), TimeUnit.MILLISECONDS);
		}
		if(multiLevelCacheProperties.getCaffeine().getInitialCapacity() > 0) {
			cacheBuilder.initialCapacity(multiLevelCacheProperties.getCaffeine().getInitialCapacity());
		}
		if(multiLevelCacheProperties.getCaffeine().getMaximumSize() > 0) {
			cacheBuilder.maximumSize(multiLevelCacheProperties.getCaffeine().getMaximumSize());
		}
		if(multiLevelCacheProperties.getCaffeine().getRefreshAfterWrite() > 0) {
			cacheBuilder.refreshAfterWrite(multiLevelCacheProperties.getCaffeine().getRefreshAfterWrite(), TimeUnit.MILLISECONDS);
		}
		return cacheBuilder.build();
	}

	@Override
	public Collection<String> getCacheNames() {
		return this.cacheNames;
	}
	
	public void clearLocal(String cacheName, Object key) {
		Cache cache = cacheMap.get(cacheName);
		if(cache == null) {
			return ;
		}
		
		RedisCaffeineCache redisCaffeineCache = (RedisCaffeineCache) cache;
		redisCaffeineCache.clearLocal(key);
	}
}
