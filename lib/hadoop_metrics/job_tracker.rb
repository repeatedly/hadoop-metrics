require 'hadoop_metrics/api'

module HadoopMetrics
  class JobTracker
    include API

    def initialize(host, port, opts = {})
      @jmx_endpoint = URI("http://#{host}:#{port}/jmx?qry=hadoop:service=JobTracker,name=JobTrackerInfo")
      @metrics_endpoint = URI("http://#{host}:#{port}/metrics?format=json")
      @json_value_fields = %W(SummaryJson AliveNodesInfoJson BlacklistedNodesInfoJson QueueInfoJson)
      @snake_case = opts.has_key?(:snake_case) ? opts[:snake_case] : true
    end

    def fairscheduler_pools(column = 'name')
      group_by('fairscheduler', 'pools', column)
    end

    def fairscheduler_jobs(column = 'name')
      group_by('fairscheduler','jobs', column)
    end

    def fairscheduler_running_tasks(target = 'pools')
      fs = metrics['fairscheduler']
      return nil if fs.nil?

      targets = fs[target]
      return nil if targets.nil?

      each_tasks = {}
      targets.each { |target|
        name = target.first['name']
        each_tasks[name] ||= 0
        each_tasks[name] += target.last['runningTasks']
      }
      each_tasks
    end
  end
end
