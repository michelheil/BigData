package com.kafkainaction;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public enum alert_status {
  Critical, Major, Minor, Warning  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"alert_status\",\"namespace\":\"com.kafkainaction\",\"symbols\":[\"Critical\",\"Major\",\"Minor\",\"Warning\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
}
