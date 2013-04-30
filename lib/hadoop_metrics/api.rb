require 'net/http'
require 'json'

module HadoopMetrics
  module API
    def initialize(host, port, opts = {})
      @endpoint = "#{host}:#{port}"
      @metrics_endpoint = URI("http://#{@endpoint}/metrics?format=json")
      @snake_case = opts[:snake_case] || true
      @name = opts[:name] || host
    end

    attr_reader :name

    def metrics
      HadoopMetrics.get_response(@metrics_endpoint)
    end

    def gc
      disable_snake_case {
        result = via_jmx('java.lang:type=GarbageCollector,name=*').map { |jmx_gc_info|
          gc_info = {'type' => (/PS Scavenge/.match(jmx_gc_info['name']) ? 'minor' : 'major')}
          gc_info['estimated_time'] = jmx_gc_info['CollectionTime']
          gc_info['count'] = jmx_gc_info['CollectionCount']
          gc_info['last_start'] = jmx_gc_info['LastGcInfo']['startTime']
          gc_info['last_duration'] = jmx_gc_info['LastGcInfo']['duration']
          gc_info
        }
      }
    end

    def via_jmx(query, json_fields = [])
      HadoopMetrics.get_response(URI("http://#{@endpoint}/jmx?qry=#{query}"))['beans'].map { |jmx_json|
        json_fields.each { |f|
          jmx_json[f] = JSON.parse(jmx_json[f])
        }
        if @snake_case
          jmx_json = HadoopMetrics.snake_cased(jmx_json)
        end

        jmx_json
      }
    end

    private

    def group_by(category, target, column)
      categories = metrics[category]
      return nil if categories.nil?

      targets = categories[target]
      return nil if targets.nil?

      targets.map { |target|
        HadoopMetrics.merge_data(target, @snake_case)
      }.group_by { |target|
        target[column]
      }
    end

    def disable_snake_case
      old_snake_case = @snake_case
      @snake_case = false

      yield
    ensure
      @snake_case = old_snake_case
    end

    def method_missing(method, *args)
      category, target = method.to_s.split('_', 2)
      group_by(category, target, *args)
    end
  end

  def self.get_response(endpoint)
    response = Net::HTTP.get_response(endpoint)
    if response.code.to_i == 200
      JSON.parse(response.body)
    else
      raise "Failed to get a response: code = #{response.code}, body = #{response.body}"
    end
  end

  def self.merge_data(data, snake_case)
    f = data.first
    f.merge!(data.last)
    snake_case ? snake_cased(f) : f
  end

  def self.to_snake_case(name)
    name[0] = name[0].chr.downcase
    name.gsub(/[A-Z]/) { |n| "_#{n.downcase}" }
  end

  def self.snake_cased(json)
    snake_cased_json = {}
    json.each_pair { |key, value|
      v = json[key]
      if v.is_a?(Hash)
        v = snake_cased(v)
      end
      snake_cased_json[HadoopMetrics.to_snake_case(key.dup)] = v
    }
    snake_cased_json
  end
end
