
# Create an archive library containing all the backend object files.

test_lib = static_library('test_lib',
   objects: backend_objs,
   )


storage_vfdtest = executable('storage_vfdtest',
  files('vfdTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)

storage_bufferedtest = executable('storage_bufferedtest',
  files('bufferedTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)

storage_kitchensinktest = executable('storage_kitchensinktest',
  files('kitchenSinkTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)

storage_aeadtest = executable('storage_aeadtest',
  files('aeadTest.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)

storage_lz4test = executable('storage_lz4test',
  files('lz4Test.c',  'framework/fileFramework.c', ),
  include_directories: [include_directories('.'), postgres_inc],
  link_args: backend_link_args,
  link_with: [test_lib, backend_link_with],
  link_depends: backend_link_depends,
)
