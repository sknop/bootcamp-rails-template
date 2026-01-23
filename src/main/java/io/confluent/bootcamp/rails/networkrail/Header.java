package io.confluent.bootcamp.rails.networkrail;

public record Header(String msg_type, String msg_queue_timestamp, String source_system_id, String original_data_source) {
    public static Header createHeader(String msg_type, String msg_queue_timestamp, String source_system_id,  String original_data_source) {
        return new Header(msg_type, msg_queue_timestamp, source_system_id, original_data_source);
    }
}
