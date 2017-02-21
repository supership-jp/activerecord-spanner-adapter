require 'pry'
require 'pry-byebug'

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'active_record/connection_adapters/spanner'

require 'minitest/spec'
require 'minitest/autorun'
