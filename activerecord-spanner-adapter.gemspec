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

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.require_paths = ["lib"]

  spec.add_dependency 'activerecord', "~> 5.0"
  spec.add_dependency 'google-cloud-spanner'
  spec.add_dependency 'google-gax', '~> 0.8'
  spec.add_development_dependency "bundler", "~> 1.14"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.6.0"
end
