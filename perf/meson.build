incmicro = include_directories('.')

subdir('micro')

gtk2_dep = dependency('gtk+-2.0', required: false)

libcairoperf = static_library('cairoperf',
  [
    'cairo-perf.c',
    'cairo-perf-report.c',
    'cairo-stats.c',
    '../src/cairo-time.c',
    'cairo-perf.h',
    'cairo-stats.h',
  ],
  include_directories: [incbase, incsrc, incboilerplate, inccairomissing],
  link_with: [libcairomissing, libcairoboilerplate],
  dependencies: [pixman_dep],
)


analyse_trace = executable('cairo-analyse-trace',
  [
    'cairo-analyse-trace.c',
    '../src/cairo-error.c',
  ],
  include_directories: [incbase, incsrc, incboilerplate, inccairoscript, inccairomissing],
  link_with: [libcairo, libcairoboilerplate, libcairoscript, libcairomissing],
  dependencies: [pixman_dep, fontconfig_dep],
)

perf_trace = executable('cairo-perf-trace',
  [
    'cairo-perf-trace.c',
    '../src/cairo-error.c',
    '../src/cairo-hash.c',
  ],
  include_directories: [incbase, incsrc, incboilerplate, inccairoscript, inccairomissing],
  link_with: [libcairoperf, libcairoscript],
  dependencies: [pixman_dep, fontconfig_dep],
)

micro = executable('cairo-perf-micro',
  [ 'cairo-perf-micro.c', ],
  include_directories: [incbase, incsrc, incboilerplate, inccairomissing],
  link_with: [libcairoperf, libcairoperfmicro],
  dependencies: [fontconfig_dep],
)

diff_files = executable('cairo-perf-diff-files',
  [
    'cairo-perf-diff-files.c',
  ],
  include_directories: [incbase, incsrc, incboilerplate],
  link_with: [libcairoperf],
)

print = executable('cairo-perf-print',
  [
    'cairo-perf-print.c',
  ],
  include_directories: [incbase, incsrc, incboilerplate],
  link_with: [libcairoperf],
)

chart = executable('cairo-perf-chart',
  [
    'cairo-perf-chart.c',
  ],
  include_directories: [incbase, incsrc, incboilerplate],
  link_with: [libcairoperf],
)

compare_backends = executable('cairo-perf-compare-backends',
  [
    'cairo-perf-compare-backends.c',
  ],
  include_directories: [incbase, incsrc, incboilerplate],
  link_with: [libcairoperf],
)

if gtk2_dep.found()
  graph_files = executable('cairo-perf-graph',
    [
      'cairo-perf-graph-files.c',
      'cairo-perf-graph-widget.c',
      'cairo-perf-graph.h',
    ],
    include_directories: [incbase, incsrc, incboilerplate],
    link_with: [libcairo, libcairoboilerplate, libcairoperf],
    dependencies: [gtk2_dep],
  )
endif
