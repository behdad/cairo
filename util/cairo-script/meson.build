cairoscript_interpreter_sources = [
  'cairo-script-file.c',
  'cairo-script-hash.c',
  'cairo-script-interpreter.c',
  'cairo-script-objects.c',
  'cairo-script-operators.c',
  'cairo-script-scanner.c',
  'cairo-script-stack.c',
]

cairoscript_interpreter_headers = [
  'cairo-script-interpreter.h',
]

csi_replay_sources = [
  'csi-replay.c',
]

csi_exec_sources = [
  'csi-exec.c',
]

csi_trace_sources = [
  'csi-trace.c',
]

libcairoscript = library('cairo-script-interpreter',
  cairoscript_interpreter_sources,
  c_args: ['-DHAVE_CONFIG_H'],
  include_directories: [incbase, incsrc],
  dependencies: deps,
  link_with: [libcairo],
  soversion: '2',
  version: '2.' + ''.join(meson.project_version().split('.')) + '.0',
  install: true,
)

inccairoscript = include_directories('.')

libcairoscript_dep = declare_dependency(link_with: libcairoscript,
  include_directories: [incbase, inccairoscript])

csi_replay_exe = executable('csi-replay', csi_replay_sources,
  c_args: ['-DHAVE_CONFIG_H'],
  include_directories: [incbase, incsrc],
  link_with: [libcairo, libcairoscript],
  dependencies: deps,
)

csi_exec_exe = executable('csi-exec', csi_exec_sources,
  c_args: ['-DHAVE_CONFIG_H'],
  include_directories: [incbase, incsrc],
  link_with: [libcairo, libcairoscript],
  dependencies: deps,
)

if feature_conf.get('CAIRO_HAS_SCRIPT_SURFACE', 0) == 1 and conf.get('HAVE_LIBGEN_H', 0) == 1
  csi_trace_exe = executable('csi-trace', csi_trace_sources,
    c_args: ['-DHAVE_CONFIG_H'],
    include_directories: [incbase, incsrc],
    link_with: [libcairo, libcairoscript],
    dependencies: deps,
  )
endif

install_headers(cairoscript_interpreter_headers, subdir: 'cairo')
