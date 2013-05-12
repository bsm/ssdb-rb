require 'spec_helper'

describe SSDB::Batch do

  let :futures do
    subject.instance_variable_get(:@futures)
  end

  let :calling do
    -> { subject.call cmd: ["incr", "key", 2] }
  end

  it { should be_a(Array) }

  describe "#call" do

    it 'should store commands' do
      calling.should change { subject.count }.by(1)
      subject.last.should include(:cmd)
    end

    it 'should remember futures' do
      calling.should change { futures.count }.by(1)
      futures.last.should be_instance_of(SSDB::Future)
    end

  end

  describe "applying values" do
    before { 3.times { calling.call } }

    it 'should set feature' do
      subject.values = [2, 4, 6]
      futures.map(&:value).should == [2, 4, 6]
    end
  end

end
