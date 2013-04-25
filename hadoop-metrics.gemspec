$:.push File.expand_path("../lib", __FILE__)
require "hadoop_metrics/version"

Gem::Specification.new do |s|
  s.name        = "hadoop-metrics"
  s.version     = HadoopMetrics::VERSION
  s.authors     = ["Masahiro Nakagawa"]
  s.email       = ["repeatedly@gmail.com"]
  s.homepage    = "https://github.com/repeatedly/hadoop-metrics"
  s.summary     = %q{Wrapper for Hadoop Metrics API}
  s.description = s.summary

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  gem.add_development_dependency "rake", ">= 0.9.2"
end
