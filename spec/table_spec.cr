require "./spec_helper"
require "json"

describe AMQ::Protocol::Table do
  it "can be encoded and decoded" do
    data = {
      "bool"    => true,
      "int8"    => Int8::MAX,
      "uint8"   => UInt8::MAX,
      "int16"   => Int16::MAX,
      "uint16"  => UInt16::MAX,
      "int32"   => Int32::MAX,
      "uint32"  => UInt32::MAX,
      "int64"   => Int64::MAX,
      "float32" => 0.0_f32,
      # "float64" => 0.0_f64,
      "string" => "a" * 257,
      "array"  => [
        true,
        Int8::MAX,
        UInt8::MAX,
        Int16::MAX,
        UInt16::MAX,
        Int32::MAX,
        UInt32::MAX,
        Int64::MAX,
        0.0_f32,
        # 0.0_f64,
        "a" * 257,
        "aaaa".to_slice,
        Time.unix(Time.utc.to_unix),
        {"key" => "value"},
        nil,
      ],
      "byte_array" => "aaaa".to_slice,
      "time"       => Time.unix(Time.utc.to_unix),
      "hash"       => {"key" => 1},
      "nil"        => nil,
    }
    tbl = AMQ::Protocol::Table.new(data)
    io = IO::Memory.new
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.pos.should eq(tbl.bytesize)
    io.pos = 0
    data2 = AMQ::Protocol::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    data2.should eq tbl
    data2.to_h.should eq data
  end

  it "can be modified" do
    tbl = AMQ::Protocol::Table.new({"key" => "value"})
    tbl.bytesize.should eq(sizeof(UInt32) + 1 + "key".bytesize + 1 + sizeof(UInt32) + "value".bytesize)
    tbl["key"] = 1
    tbl.bytesize.should eq(sizeof(UInt32) + 1 + "key".bytesize + 1 + sizeof(Int32))
    io = IO::Memory.new
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.rewind
    tbl2 = AMQ::Protocol::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    tbl2.should eq tbl
    tbl2.bytesize.should eq(sizeof(UInt32) + 1 + "key".bytesize + 1 + sizeof(Int32))
  end

  it "comparision is semantic not per byte" do
    t1 = AMQ::Protocol::Table.new(Hash(String, AMQ::Protocol::Field){
      "a" => 1_u8,
      "b" => 1_u8,
    })
    t2 = AMQ::Protocol::Table.new(Hash(String, AMQ::Protocol::Field){
      "b" => 1_i32,
      "a" => 1_i64,
    })
    t1.should eq t2
  end

  it "can be created from namedtuple" do
    t1 = AMQ::Protocol::Table.new({b: 1_i32, a: 1_i64})
    t2 = AMQ::Protocol::Table.new(Hash(String, AMQ::Protocol::Field){
      "b" => 1_i32,
      "a" => 1_i64,
    })
    t1.should eq t2
  end

  it "can be encoded and decoded JSON::Any" do
    data = JSON.parse("{ \"a\": 1, \"b\": \"string\", \"c\": 0.2, \"d\": [1,2], \"e\": null }").as_h
    tbl = AMQ::Protocol::Table.new(data)
    tbl.to_h.should eq({"a" => 1, "b" => "string", "c" => 0.2, "d" => [1, 2], "e" => nil})
    io = IO::Memory.new
    io.write_bytes tbl, IO::ByteFormat::NetworkEndian
    io.pos.should eq(tbl.bytesize)
    io.pos = 0
    data2 = AMQ::Protocol::Table.from_io(io, IO::ByteFormat::NetworkEndian)
    data2.should eq tbl
  end

  it "can be cloned" do
    t1 = AMQ::Protocol::Table.new({b: 1_i32, a: 1_i64})
    t2 = t1.clone
    t2.should eq t1
    t2["c"] = 2
    t2.should_not eq t1
  end

  it "can be json serialized" do
    data = {a: 1, b: "string", c: 0.2, d: [1, 2], e: nil}
    t1 = AMQ::Protocol::Table.new(data)
    json = t1.to_json
    json.should eq data.to_json
  end

  it "can be empty" do
    t1 = AMQ::Protocol::Table.new(nil)
    t1.should be_empty
  end

  it "supports #delete" do
    t1 = AMQ::Protocol::Table.new({a: 1, b: "foo"})
    t1.delete("a").should eq 1
    t1["a"]?.should be_nil
    t1.to_h.should eq({"b" => "foo"})
  end

  it "supports #reject!" do
    t1 = AMQ::Protocol::Table.new({a: 1, b: "foo"})
    t1.reject! { |k, v| k.in?("a") }
    t1.to_h.should eq({"b" => "foo"})
  end

  describe "#merge!" do
    it "supports Table" do
      t1 = AMQ::Protocol::Table.new({a: 1, b: "foo"})
      t2 = AMQ::Protocol::Table.new({c: nil})
      t1.merge!(t2)
      t1.to_h.should eq({"a" => 1, "b" => "foo", "c" => nil})
    end

    it "supports NamedTuple" do
      t1 = AMQ::Protocol::Table.new({a: 1, b: "foo"})
      t1.merge!({c: nil, b: "bar"})
      t1["a"].should eq 1
      t1["b"].should eq "bar"
      t1["c"].should eq nil
      t1.size.should eq 3
      t1.to_h.should eq({"a" => 1, "b" => "bar", "c" => nil})
    end

    it "supports Hash(String, Field)" do
      t1 = AMQ::Protocol::Table.new({a: 1, b: "foo"})
      t1.merge!({"c" => nil} of String => AMQ::Protocol::Field)
      t1.to_h.should eq({"a" => 1, "b" => "foo", "c" => nil})
    end
  end

  it "can add fields" do
    t1 = AMQ::Protocol::Table.new({"foo": "bar"})
    t1["x-stream-offset"] = 1i64
    t1["x-delay"]?.should be_nil
    t1.to_h.should eq({"foo" => "bar", "x-stream-offset" => 1i64})
  end

  it "can add to fields to empty" do
    t1 = AMQ::Protocol::Table.new
    t1["x-stream-offset"] = 1i64
    t1["x-delay"]?.should be_nil
    t1.to_h.should eq({"x-stream-offset" => 1i64})
  end
end

# Verifies bugfix for Sub-table memory corruption
# https://github.com/cloudamqp/amq-protocol.cr/pull/14
describe "should not overwrite sub-tables memory when reassigning values in a Table" do
  it "when reassigning a Table" do
    parent_table = AMQ::Protocol::Table.new
    child_table = AMQ::Protocol::Table.new({"a": "b"})
    parent_table["table"] = child_table

    # Read child_table from io
    table_from_io = parent_table["table"].as(AMQ::Protocol::Table)

    # Overwrite child_table data in parent_table
    parent_table["table"] = "foo"

    # Verify that read_table wasn't modified by reassignment of "table"
    table_from_io.should eq child_table
  end

  it "when reassigning a string" do
    parent_table = AMQ::Protocol::Table.new
    child_table = AMQ::Protocol::Table.new({"abc": "123"})

    parent_table["foo"] = "bar"
    parent_table["tbl"] = child_table

    # Read child_table from io
    read_table = parent_table["tbl"].as(AMQ::Protocol::Table)

    # Overwrite string "foo" in parent_table
    parent_table["foo"] = "foooo"

    # Verify that read_table wasn't modified by reassignment of "foo"
    read_table.should eq child_table
  end
end
