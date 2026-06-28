/*
    Statcast strike zone grid:
    Zones 1-9  = strike zone (3x3 grid, top-left to bottom-right)
    Zones 11-14 = out of zone (quadrants around the strike zone)
*/

select zone_id, zone_description, is_in_zone, zone_region from (
    values
        (1,  'Top-Left',        1, 'Upper'),
        (2,  'Top-Center',      1, 'Upper'),
        (3,  'Top-Right',       1, 'Upper'),
        (4,  'Middle-Left',     1, 'Middle'),
        (5,  'Middle-Center',   1, 'Heart'),
        (6,  'Middle-Right',    1, 'Middle'),
        (7,  'Bottom-Left',     1, 'Lower'),
        (8,  'Bottom-Center',   1, 'Lower'),
        (9,  'Bottom-Right',    1, 'Lower'),
        (11, 'Above-Left',      0, 'Chase-Up'),
        (12, 'Above-Right',     0, 'Chase-Up'),
        (13, 'Below-Left',      0, 'Chase-Down'),
        (14, 'Below-Right',     0, 'Chase-Down')
) as z(zone_id, zone_description, is_in_zone, zone_region)
