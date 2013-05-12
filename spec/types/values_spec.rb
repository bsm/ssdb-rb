require 'spec_helper'

describe SSDB do
  describe "plain values" do

    before do
      subject.set "#{FPX}:key1", 1
      subject.set "#{FPX}:key2", "2"
      subject.set "#{FPX}:key3", "C"
      subject.set "#{FPX}:key4", "d"
    end

    it "should check existence" do
      subject.exists("#{FPX}:key1").should be(true)
      subject.exists?("#{FPX}:key2").should be(true)
      subject.exists?("#{FPX}:keyX").should be(false)
    end

    it "should delete" do
      -> {
        subject.del("#{FPX}:key1").should be_nil
      }.should change { subject.exists?("#{FPX}:key1") }.to(false)

      subject.del("#{FPX}:keyX").should be_nil
    end

    it "should list" do
      subject.keys("#{FPX}:key1", "#{FPX}:key3").should == ["#{FPX}:key2", "#{FPX}:key3"]
      subject.keys("#{FPX}:key0", "#{FPX}:keyz", limit: 2).should == ["#{FPX}:key1", "#{FPX}:key2"]
      subject.keys("#{FPX}:keyy", "#{FPX}:keyz").should == []
      subject.keys("#{FPX}:keyz", "#{FPX}:key0").should == []
    end

    it "should scan" do
      subject.scan("#{FPX}:key1", "#{FPX}:key3").should == [["#{FPX}:key2", "2"], ["#{FPX}:key3", "C"]]
      subject.scan("#{FPX}:key0", "#{FPX}:keyz", limit: 2).should == [["#{FPX}:key1", "1"], ["#{FPX}:key2", "2"]]
      subject.scan("#{FPX}:keyy", "#{FPX}:keyz").should == []
      subject.scan("#{FPX}:keyz", "#{FPX}:key0").should == []
    end

    it "should rscan" do
      subject.rscan("#{FPX}:key3", "#{FPX}:key1").should == [["#{FPX}:key2", "2"], ["#{FPX}:key1", "1"]]
      subject.rscan("#{FPX}:keyz", "#{FPX}:key0", limit: 2).should == [["#{FPX}:key4", "d"], ["#{FPX}:key3", "C"]]
      subject.rscan("#{FPX}:keyz", "#{FPX}:keyy").should == []
      subject.rscan("#{FPX}:key0", "#{FPX}:keyz").should == []
    end

    it "should set/get values" do
      subject.set("#{FPX}:key", "a").should be(true)
      subject.set("#{FPX}:key", "a").should be(true)
      subject.set("#{FPX}:key", "b").should be(true)

      subject.get("#{FPX}:key").should == "b"
      subject.get("#{FPX}:keyX").should be_nil
    end

    it "should increment/decrement values" do
      subject.incr("#{FPX}:key", 7).should == 7
      subject.decr("#{FPX}:key", 2).should == 5
      subject.incr("#{FPX}:key", 4).should == 9
      subject.incr("#{FPX}:key", 3.9).should == 12
      subject.decr("#{FPX}:key", 2.8).should == 10

      subject.set "#{FPX}:keyN", "100"
      subject.incr("#{FPX}:keyN", 2).should == 102

      subject.set "#{FPX}:keyN", "100"
      subject.decr("#{FPX}:keyN", 2).should == 98

      subject.set "#{FPX}:keyN", "5.9"
      subject.incr("#{FPX}:keyN", 3.1).should == 8

      subject.set "#{FPX}:keyS", "a"
      subject.incr("#{FPX}:keyS", 4).should == 4
      subject.get("#{FPX}:keyS").should == "4"

      subject.set "#{FPX}:keyS", "a"
      subject.decr("#{FPX}:keyS", 4).should == -4
      subject.get("#{FPX}:keyS").should == "-4"
    end

    it 'should multi-get/set' do
      subject.multi_set("#{FPX}:key4" => "x", "#{FPX}:key8" => "y", "#{FPX}:key9" => "z").should == 6
      subject.multi_get(["#{FPX}:key4", "#{FPX}:key6", "#{FPX}:key9", "#{FPX}:key8"]).
        should == ["x", nil, "z", "y"]
    end

    it 'should check existence of multiple keys' do
      subject.multi_exists(["#{FPX}:key2", "#{FPX}:key3"]).should == [true, true]
      subject.multi_exists(["#{FPX}:key2", "#{FPX}:key9", "#{FPX}:key3"]).should == [true, false, true]
    end

    it 'should multi-delete' do
      keys = ["#{FPX}:key2", "#{FPX}:key3", "#{FPX}:key9"]
      -> {
        subject.multi_del(keys).should == 0
      }.should change {
        subject.multi_exists(keys)
      }.from([true, true, false]).to([false, false, false])
    end

  end
end
