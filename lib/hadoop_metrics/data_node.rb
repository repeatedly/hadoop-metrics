require "hadoop_metrics/api"

module HadoopMetrics
  class DataNode
    include API

    NODE_TYPE = 'dn'
    JSON_FILED_VALUES = %W(NamenodeAddresses VolumeInfo)

    def info
      query_jmx('Hadoop:service=DataNode,name=DataNodeInfo', JSON_FILED_VALUES).first
    end
  end
end
