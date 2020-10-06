cairo_gobject_sources = [
  'cairo-gobject-enums.c',
  'cairo-gobject-structs.c',
]

cairo_gobject_headers = [
  'cairo-gobject.h',
]

libcairogobject = library('cairo-gobject', cairo_gobject_sources,
  c_args: ['-DHAVE_CONFIG_H'],
  include_directories: [incbase, incsrc],
  dependencies: [glib_dep, gobject_dep],
  link_with: [libcairo],
  soversion: '2',
  version: '2.' + ''.join(meson.project_version().split('.')) + '.0',
  install: true,
)

incgobject = include_directories('.')

libcairogobject_dep = declare_dependency(link_with: libcairogobject,
  include_directories: [incgobject],
  dependencies: libcairo_dep)

pkgmod.generate(libcairogobject,
  libraries: [libcairo, glib_dep, gobject_dep],
  description: 'cairo-gobject for cairo graphics library',
  subdirs: [meson.project_name()],
)

meson.override_dependency('cairo-gobject', libcairogobject_dep)

install_headers(cairo_gobject_headers, subdir: 'cairo')
