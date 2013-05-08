require 'hadoop_metrics/api'

module HadoopMetrics
  class NameNode
    include API

    JSON_FILED_VALUES = %W(LiveNodes DeadNodes DecomNodes NameDirStatuses)

    def info
      via_jmx('Hadoop:service=NameNode,name=NameNodeInfo', JSON_FILED_VALUES).first
    end

    def dfs
      via_jmx('Hadoop:service=NameNode,name=FSNamesystem').first
    end
  end
end
