package io.conduktor.demos.kafka.wikimedia.consumer.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikimediaRecentChange {
	@JsonProperty("$schema")
	private String schema;

	private Meta meta;

	private Long id;

	private String type;

	private Integer namespace;

	private String title;

	@JsonProperty("title_url")
	private String titleUrl;

	private String comment;

	private Long timestamp;

	private String user;

	private Boolean bot;

	@JsonProperty("notify_url")
	private String notifyUrl;

	@JsonProperty("server_url")
	private String serverUrl;

	@JsonProperty("server_name")
	private String serverName;

	@JsonProperty("server_script_path")
	private String serverScriptPath;

	private String wiki;

	@JsonProperty("parsedcomment")
	private String parsedComment;

	// Inner Class for Meta
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Meta {

		private String uri;

		@JsonProperty("request_id")
		private String requestId;

		private String id;

		private String domain;

		private String stream;

		private String dt;

		private String topic;

		private Integer partition;

		private Long offset;
	}
}
