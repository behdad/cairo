libpdiff_sources = [
  'lpyramid.c',
  'pdiff.c',
]

perceptualdiff_sources = [
  'args.c',
  'perceptualdiff.c',
]

libpdiff = static_library('pdiff', libpdiff_sources,
  include_directories: [incbase, incsrc],
  dependencies: deps,
  link_with: [libcairo],
)

perceptualdiff = executable('perceptualdiff', perceptualdiff_sources,
  include_directories: [incbase, incsrc],
  link_with: [libpdiff])
