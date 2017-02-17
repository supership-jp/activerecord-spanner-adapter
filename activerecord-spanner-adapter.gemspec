# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'activerecord-spanner-adapter/version'

Gem::Specification.new do |spec|
  spec.name          = "activerecord-spanner-adapter"
  spec.version       = ActiveRecordSpannerAdapter::VERSION
  spec.authors       = ["Yuki Yugui Sonoda"]
  spec.email         = ["yuki.sonoda@supership.jp"]

  spec.summary       = %q{Adapts Google Cloud Spanner to ActiveRecord}
  spec.description   = %q{Connection Adapter of Google Cloud Spanner to ActiveRecord O/R mapper library}
  spec.homepage      = "https://github.com/supership-jp/activerecord-spanner-adapter"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.require_paths = ["lib"]

  spec.add_dependency 'activerecord', "~> 5.0"
  spec.add_dependency 'google-cloud-spanner'
  spec.add_development_dependency "bundler", "~> 1.14"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", "~> 5.0"
end
