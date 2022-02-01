require "./spec_helper"

describe AMQ::Protocol::VERSION do
  it "version matches shard version" do
    AMQ::Protocol::VERSION == {{ `shards version`.stringify }}
  end
end
