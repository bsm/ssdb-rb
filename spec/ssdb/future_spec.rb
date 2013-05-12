require 'spec_helper'

describe SSDB::Future do

  subject do
    described_class.new ["set", "key", "val"]
  end

  it { should be_instance_of(described_class) }

  it "should be introspectable" do
    subject.inspect.should == %(<SSDB::Future ["set", "key", "val"]>)
  end

  it "should raise error when not ready" do
    -> { subject.value }.should raise_error(SSDB::FutureNotReady)
  end

  it "should return value when ready" do
    subject.value = "ok"
    subject.value.should == "ok"

    subject.value = true
    subject.value.should == true

    subject.value = nil
    subject.value.should == nil
  end

end
