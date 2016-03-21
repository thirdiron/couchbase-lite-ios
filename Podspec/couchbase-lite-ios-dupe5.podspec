Pod::Spec.new do |s|
  s.name            = 'couchbase-lite-ios'
  s.version         = '1.2.1'
  s.license         = { :type => 'Apache License, Version 2.0', :file => 'LICENSE.txt' }
  s.summary         = 'An embedded syncable NoSQL database for iOS apps.'
  s.homepage        = 'http://mobile.couchbase.com'
  s.author          = 'Couchbase'
  s.source = { :http => "https://github.com/thirdiron/couchbase-lite-ios/releases/download/v1.2.1-pre-release-5/couchbase-lite-ios-community_1.2.1-7.zip" }
  s.preserve_paths  = 'LICENSE.txt'
  s.ios.deployment_target = '7.0'
  s.frameworks      = 'CFNetwork', 'Security', 'SystemConfiguration'
  s.libraries       = 'sqlite3', 'z'
  s.xcconfig        = { 'OTHER_LDFLAGS' => '-ObjC' }
  s.default_subspec = 'Core'

  s.subspec 'Core' do |ss|
    ss.preserve_paths = 'CouchbaseLite.framework'
    ss.vendored_frameworks = 'CouchbaseLite.framework'
  end

  s.subspec 'Listener' do |ss|
    ss.dependency 'couchbase-lite-ios/Core'

    ss.preserve_paths = 'CouchbaseLiteListener.framework'
    ss.vendored_frameworks = 'CouchbaseLiteListener.framework'
  end
end
