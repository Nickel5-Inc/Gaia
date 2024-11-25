import random
import h3
import json
from shapely.geometry import Polygon, box


def h3_cell_to_polygon(h3_address):
    """Convert H3 cell to Shapely polygon."""
    boundary = h3.cell_to_boundary(h3_address)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])
    return Polygon(coords)


def select_random_base_cell(base_cells, resolution=2, min_lat=-56, max_lat=60):
    """Randomly select a large base cell at the specified resolution within latitude limits."""
    eligible_cells = [
        cell
        for cell in base_cells
        if cell["resolution"] == resolution
        and min_lat <= h3.cell_to_latlng(cell["index"])[0] <= max_lat
    ]
    return random.choice(eligible_cells) if eligible_cells else None


def subdivide_if_urban_present(
    base_hex, urban_cells_set, lakes_cells_set, target_resolution=5, max_resolution=8
):
    """Subdivide a base cell only if it overlaps with urban cells, filtering out hexagons that overlap urban or water areas."""
    if base_hex in urban_cells_set:
        subdivisions = h3.cell_to_children(base_hex, target_resolution)
        filtered_cells = []

        for hex_id in subdivisions:
            if hex_id in urban_cells_set or hex_id in lakes_cells_set:
                continue

            hex_polygon = h3_cell_to_polygon(hex_id)
            hex_bounds = hex_polygon.bounds

            if (
                hex_bounds[2] - hex_bounds[0] < 1.0
                or hex_bounds[3] - hex_bounds[1] < 1.0
            ) and target_resolution < max_resolution:
                filtered_cells.extend(
                    subdivide_if_urban_present(
                        hex_id,
                        urban_cells_set,
                        lakes_cells_set,
                        target_resolution + 1,
                        max_resolution,
                    )
                )
            else:
                filtered_cells.append(hex_id)

        return filtered_cells
    else:
        return [base_hex]


def largest_inscribed_square(hex_polygon):
    """Calculate the largest inscribed square within a hexagon polygon."""
    bounds = hex_polygon.bounds
    max_square_size = min(bounds[2] - bounds[0], bounds[3] - bounds[1])
    center_x, center_y = hex_polygon.centroid.x, hex_polygon.centroid.y
    square_size = max_square_size

    while square_size > 0:
        inscribed_square = box(
            center_x - square_size / 2,
            center_y - square_size / 2,
            center_x + square_size / 2,
            center_y + square_size / 2,
        )
        if hex_polygon.contains(inscribed_square):
            return inscribed_square
        square_size -= 0.01

    return None


def select_1x1_degree_box_in_square(square, hex_polygon):
    """Position a 1x1 degree box within the inscribed square, centered if possible."""
    center_x, center_y = square.centroid.x, square.centroid.y
    bbox = box(center_x - 0.5, center_y - 0.5, center_x + 0.5, center_y + 0.5)

    if hex_polygon.contains(bbox):
        return bbox.bounds
    else:
        return None


def select_random_region(
    base_cells, urban_cells_set, lakes_cells_set, min_lat=-56, max_lat=60
):
    """Randomly select a 1x1 degree box that does not overlap urban or lake cells within given lat/lon bounds."""
    base_cell = select_random_base_cell(
        base_cells, resolution=2, min_lat=min_lat, max_lat=max_lat
    )

    if not base_cell:
        raise ValueError("No eligible base cells found within the specified bounds.")
    filtered_cells = subdivide_if_urban_present(
        base_cell["index"], urban_cells_set, lakes_cells_set, target_resolution=5
    )

    for hex_id in filtered_cells:
        hex_polygon = h3_cell_to_polygon(hex_id)
        inscribed_square = largest_inscribed_square(hex_polygon)

        if not inscribed_square:
            continue

        valid_bbox = select_1x1_degree_box_in_square(inscribed_square, hex_polygon)

        if valid_bbox:
            return valid_bbox

    raise ValueError("No valid 1x1 degree box found in the selected region.")
