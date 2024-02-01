# Generate doxygen pages for PostgreSQL using "ninja doxygen"
#
# Doxygen pages are optional. Nothing in this script should
# cause PostgreSQL builds to fail.
#
# Currently there are no explicit error messages
#   - If doxygen is not found, the doxygen target will not be defined.
#   - If dot is not found, no graphs will be generated.
#   - flex is already required, so we don't check for it.
#
# As a future enhancement, display meaningful error messages
# when typing "ninja doxygen". Meson can display these messages
# using a python custom target.
#
# To generate graphs, install graphviz and configure with
#     "meson configure -Ddoxygen_graphs=true"
# Generating graphs can be slow, so they are off by default.
#

# Check our environment first.
doxygen_cmd = find_program('doxygen', required: false, native: true)
dot_cmd = find_program('dot', required: false, native: true)
doxygen_graphs = get_option('doxygen_graphs')

# Do nothing if doxygen or flex are not found
if not doxygen_cmd.found()
    subdir_done()
endif

# build the doxygen 'C' filter from its flex source
doxygen_filter_c = custom_target(
    'doxy_filter_c',
    build_by_default: false,
    input: 'doxy_filter.l',
    output: 'doxy_filter.c',
    command: [flex_cmd])  # Add '--', '-d' to debug flex filter
doxygen_filter_exe = executable(
    'doxy_filter',
    doxygen_filter_c,
    build_by_default: false)

# Point to the source code directories
doxygen_source_dirs = [
    join_paths(meson.source_root(), 'src'),
    join_paths(meson.source_root(), 'contrib'),
]

# Configure the Doxyfile
doxygen_config = configuration_data()
doxygen_config.set('INPUT', ' '.join(doxygen_source_dirs))
doxygen_config.set('OUTPUT_DIRECTORY', meson.current_build_dir())
doxygen_config.set('PROJECT_NUMBER', pg_version)
doxygen_config.set('INCLUDE_PATH', ' '.join(postgres_inc_d))
doxygen_config.set('INPUT_FILTER', doxygen_filter_exe.full_path())
doxygen_config.set('STRIP_FROM_PATH', meson.source_root())
if (doxygen_graphs and dot_cmd.found())
    doxygen_config.set('GRAPHS', 'YES')
else
    doxygen_config.set('GRAPHS', 'NO')
endif

doxyfile = configure_file(input : 'Doxyfile.in',
                          output : 'Doxyfile',
                          configuration : doxygen_config)

# Create a target to remove old doxygen html pages.
# If not removed, the directory will have a confusing mix of old and new pages.
# To avoid a circular dependency, generate the html path here
# rather than getting it from doxygen_html.
doxygen_html_dir = join_paths(meson.current_build_dir(), 'html')
doxygen_clean = custom_target(
    'doxygen_clean',
    output : 'doxygen_clean',
    command : [python, '-c', 'from shutil import rmtree; rmtree("' + doxygen_html_dir + '", ignore_errors=True)'],
    build_always_stale: true,
    build_by_default: false)

# Create a target to generate doxygen html pages
# after cleaning up the old pages.
doxygen_html = custom_target(
    'html',
    input : doxyfile,
    output : 'html',
    command : [doxygen_cmd, doxyfile],
    depends: [doxygen_filter_exe, doxygen_clean],
    build_always_stale: true,
    build_by_default: false)

# use "ninja doxygen" to build doxygen pages.
alias_target('doxygen', [doxygen_html])
