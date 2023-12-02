package com.brucky.cache.autoconfigure;

import com.brucky.cache.prop.MultiLevelCacheProperties;
import com.brucky.cache.support.CacheMessageListener;
import com.brucky.cache.support.RedisCaffeineCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.net.UnknownHostException;

/**  
 * 配置注入类
 * @version 1.0.0
 */
@Configuration
@AutoConfigureAfter(RedisAutoConfiguration.class)
@EnableConfigurationProperties(MultiLevelCacheProperties.class)
public class MultiLevelCacheAutoConfiguration {
	
	@Autowired
	private MultiLevelCacheProperties multiLevelCacheProperties;
	
	@Bean
	@ConditionalOnBean(RedisTemplate.class)
	public RedisCaffeineCacheManager cacheManager(RedisTemplate<Object, Object> stringKeyRedisTemplate) {
		return new RedisCaffeineCacheManager(multiLevelCacheProperties, stringKeyRedisTemplate);
	}
	
	@Bean
	@ConditionalOnMissingBean(name = "stringKeyRedisTemplate")
	public RedisTemplate<Object, Object> stringKeyRedisTemplate(RedisConnectionFactory redisConnectionFactory) throws UnknownHostException {
		RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<Object, Object>();
		redisTemplate.setConnectionFactory(redisConnectionFactory);
		redisTemplate.setKeySerializer(RedisSerializer.string());
		redisTemplate.setValueSerializer(RedisSerializer.json());
		redisTemplate.setHashKeySerializer(RedisSerializer.string());
		redisTemplate.setHashValueSerializer(RedisSerializer.json());
		return redisTemplate;
	}
	
	@Bean
	public RedisMessageListenerContainer redisMessageListenerContainer(RedisTemplate<Object, Object> stringKeyRedisTemplate,
																	   RedisCaffeineCacheManager redisCaffeineCacheManager) {
		RedisMessageListenerContainer redisMessageListenerContainer = new RedisMessageListenerContainer();
		redisMessageListenerContainer.setConnectionFactory(stringKeyRedisTemplate.getConnectionFactory());
		CacheMessageListener cacheMessageListener = new CacheMessageListener(stringKeyRedisTemplate, redisCaffeineCacheManager);
		redisMessageListenerContainer.addMessageListener(cacheMessageListener, new ChannelTopic(multiLevelCacheProperties.getRedis().getTopic()));
		return redisMessageListenerContainer;
	}
}
