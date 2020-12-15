#include "cairo-test.h"

static cairo_test_status_t
draw (cairo_t *cr, int width, int height)
{
    cairo_set_source_rgb (cr, 0., 0., 0.);
    cairo_paint (cr);

    cairo_set_source_rgb (cr, 1., 1., 1.);
    cairo_set_line_width (cr, 1.);

    cairo_pattern_t *p = cairo_pattern_create_linear (0, 0, width, height);
    cairo_pattern_add_color_stop_rgb (p, 0, 0.99, 1, 1);
    cairo_pattern_add_color_stop_rgb (p, 1, 1, 1, 1);
    cairo_set_source (cr, p);

    cairo_move_to (cr, 0.5, -1);
    for (int i = 0; i < width; i+=3) {
	cairo_rel_line_to (cr, 2, 2);
	cairo_rel_line_to (cr, 1, -2);
    }

    cairo_set_operator (cr, CAIRO_OPERATOR_SOURCE);
    cairo_stroke (cr);

    cairo_pattern_destroy(p);

    return CAIRO_TEST_SUCCESS;
}


CAIRO_TEST (bug_image_compositor,
	    "Crash in image-compositor",
	    "stroke, stress", /* keywords */
	    NULL, /* requirements */
	    10000, 1,
	    NULL, draw)
	    
	    
