require "hadoop_metrics/api"

module HadoopMetrics
  class NameNode
    include API

    NODE_TYPE = 'nn'
    JSON_FILED_VALUES = %W(LiveNodes DeadNodes DecomNodes NameDirStatuses)

    def info
      query_jmx('Hadoop:service=NameNode,name=NameNodeInfo', JSON_FILED_VALUES).first
    end

    def dfs
      query_jmx('Hadoop:service=NameNode,name=FSNamesystem').first
    end
  end
end
