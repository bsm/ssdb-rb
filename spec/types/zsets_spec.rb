require 'spec_helper'

describe SSDB do
  describe "zsets" do

    before do
      subject.zset "#{FPX}:zset1", "a", 1
      subject.zset "#{FPX}:zset1", "b", 2
      subject.zset "#{FPX}:zset1", "c", 2.5
      subject.zset "#{FPX}:zset1", "d", 3
      subject.zset "#{FPX}:zset2", "xa", 100
      subject.zset "#{FPX}:zset2", "xz", 200
      subject.zset "#{FPX}:zset3", "za", 5
      subject.zset "#{FPX}:zset3", "zz", 5
    end

    it "should check existence" do
      subject.zexists?("#{FPX}:zset1").should be(true)
      subject.zexists("#{FPX}:zset2").should be(true)
      subject.zexists?("#{FPX}:zset9").should be(false)
    end

    it "should count" do
      subject.zsize("#{FPX}:zset1").should == 4
      subject.zsize("#{FPX}:zset2").should == 2
      subject.zsize("#{FPX}:zset9").should == 0
    end

    it "should delete" do
      -> {
        subject.zdel("#{FPX}:zset1", "a").should be(true)
      }.should change { subject.zsize("#{FPX}:zset1") }.by(-1)

      -> {
        subject.zdel("#{FPX}:zset1", "e").should be(false)
      }.should_not change { subject.zsize("#{FPX}:zset1") }
    end

    it "should set/get scores" do
      subject.zset("#{FPX}:zset9", "i", 50).should be(true)
      subject.zset("#{FPX}:zset9", "i", 60).should be(false)
      subject.zset("#{FPX}:zset9", "i", 60).should be(false)

      subject.zget("#{FPX}:zset9", "i").should == 60
      subject.zget("#{FPX}:zset1", "c").should == 2
      subject.zget("#{FPX}:zset1", "x").should be_nil
      subject.zget("#{FPX}:zsetX", "any").should be_nil
    end

    it "should 'add' scores" do
      subject.zadd("#{FPX}:zset9", 50, "i").should be(true)
      subject.zget("#{FPX}:zset9", "i").should == 50
    end

    it "should increment/decrement scores" do
      subject.zincr("#{FPX}:zset1", "a", 7).should == 8
      subject.zdecr("#{FPX}:zset1", "a", 3).should == 5
      subject.zincr("#{FPX}:zset1", "a", 3.9).should == 8
      subject.zdecr("#{FPX}:zset1", "a", 1.8).should == 7
      subject.zincr("#{FPX}:zset1", "b", 1).should == 3

      subject.zincr("#{FPX}:zset1", "e", 5).should == 5
      subject.zget("#{FPX}:zset1", "e").should == 5
    end

    it "should list sets" do
      subject.zlist("#{FPX}:zset1", "#{FPX}:zset3").should == ["#{FPX}:zset2", "#{FPX}:zset3"]
      subject.zlist("#{FPX}:zset0", "#{FPX}:zsetz", limit: 2).should == ["#{FPX}:zset1", "#{FPX}:zset2"]
      subject.zlist("#{FPX}:zsety", "#{FPX}:zsetz").should == []
      subject.zlist("#{FPX}:zsetz", "#{FPX}:zset0").should == []
    end

    it "should scan scores" do
      subject.zscan("#{FPX}:zset1", 0, 10).should == [["a", 1], ["b", 2], ["c", 2], ["d", 3]]
      subject.zscan("#{FPX}:zset1", 2, 3).should == [["b", 2], ["c", 2]]
      subject.zscan("#{FPX}:zset1", 2, 3, limit: 2).should == [["b", 2], ["c", 2]]
      subject.zscan("#{FPX}:zset1", 3, 2).should == []
      subject.zscan("#{FPX}:zset3", 0, 10).should == [["za", 5], ["zz", 5]]
      subject.zscan("#{FPX}:zsety", 0, 100).should == []
    end

    it "should rscan scores" do
      subject.zrscan("#{FPX}:zset2", 1000, 0).should == [["xz", 200], ["xa", 100]]
      subject.zrscan("#{FPX}:zset2", 0, 1000).should == []
      subject.zrscan("#{FPX}:zset2", 200, 100).should == [["xa", 100]]
    end

    it "should list score keys" do
      subject.zkeys("#{FPX}:zset1", 0, 10).should == ["a", "b", "c", "d"]
      subject.zkeys("#{FPX}:zset1", 10, 0).should == []
      subject.zkeys("#{FPX}:zset3", 0, 10).should == ["za", "zz"]
    end

    it "should check existence of multiple sets" do
      subject.multi_zexists(["#{FPX}:zset1", "#{FPX}:zset2", "#{FPX}:zset9"]).should == [true, true, false]
    end

    it "should check sizes of multiple sets" do
      subject.multi_zsize(["#{FPX}:zset1", "#{FPX}:zset2", "#{FPX}:zset9"]).should == [4, 2, 0]
    end

    it "should get/set multiple members" do
      subject.multi_zset("#{FPX}:zset5", {"k" => 7, "l" => 8}).should == 2
      subject.multi_zget("#{FPX}:zset1", ["a", "d", "x"]).should == [1, 3, 0]
      subject.multi_zget("#{FPX}:zset5", ["i", "k", "l"]).should == [0, 7, 8]
      subject.multi_zget("#{FPX}:zset9", ["x", "y"]).should == [0, 0]
    end

    it "should delete multiple members" do
      -> {
        subject.multi_zdel("#{FPX}:zset1", ["a", "d", "x"]).should == 2
      }.should change { subject.zsize("#{FPX}:zset1") }.by(-2)
    end

  end
end
