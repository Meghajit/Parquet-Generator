// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: TestMessage.proto

package parquet.generator;

public interface TestMessageOrBuilder extends
    // @@protoc_insertion_point(interface_extends:parquet.generator.TestMessage)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string query = 1;</code>
   */
  java.lang.String getQuery();
  /**
   * <code>optional string query = 1;</code>
   */
  com.google.protobuf.ByteString
      getQueryBytes();

  /**
   * <code>optional int32 page_number = 2;</code>
   */
  int getPageNumber();

  /**
   * <code>optional int32 result_per_page = 3;</code>
   */
  int getResultPerPage();
}
