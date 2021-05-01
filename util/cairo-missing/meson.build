cairo_missing_sources = [
  'strndup.c',
  'getline.c',
]

inccairomissing = include_directories('.')

libcairomissing = static_library('cairo-missing', cairo_missing_sources,
  include_directories: [incbase, incsrc],
  install: false,
  build_by_default: false,
)
