cairo_missing_sources = [
  'strndup.c',
  'getline.c',
]

inccairomissing = include_directories('.')

libcairomissing = static_library('cairo-missing', cairo_missing_sources,
  c_args: ['-DHAVE_CONFIG_H'],
  include_directories: [incbase, incsrc],
  install: false,
  build_by_default: false,
)
