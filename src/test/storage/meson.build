####################################################################
# Create an archive library containing all the backend object files.
####################################################################
test_lib = static_library('test_lib',
   objects: backend_objs,
   )


###################################################
# Individual unit tests in the 'iostack' test suite.
#  TODO: refactor so each is a single line?
###################################################

# Test the basic Vfd layer
storage_vfdtest = executable('storage_vfdtest',
  files('vfdTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
test('iostack/RawVfds', storage_vfdtest, suite: 'iostack')

# Buffering
storage_bufferedtest = executable('storage_bufferedtest',
  files('bufferedTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
test('iostack/buffering', storage_bufferedtest, suite: 'iostack')

# Encryption.
storage_aeadtest = executable('storage_aeadtest',
  files('aeadTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
test('iostack/encryption', storage_aeadtest, suite: 'iostack')

# Lz4 Compression
storage_lz4test = executable('storage_lz4test',
  files('lz4Test.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
test('iostack/compression', storage_lz4test, suite: 'iostack')

# 'Kitchen Sink' - all layers combined together.
storage_kitchensinktest = executable('storage_kitchensinktest',
  files('kitchenSinkTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
test('iostack/kitchensink', storage_kitchensinktest, suite: 'iostack')
