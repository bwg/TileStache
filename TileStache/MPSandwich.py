""" Layered, composite rendering for TileStache.

The Sandwich Provider supplies a Photoshop-like rendering pipeline, making it
possible to use the output of other configured tile layers as layers or masks
to create a combined output. Sandwich is modeled on Lars Ahlzen's TopOSM.

The external "Blit" library is required by Sandwich, and can be installed
via Pip, easy_install, or directly from Github:

    https://github.com/migurski/Blit

The "stack" configuration parameter describes a layer or stack of layers that
can be combined to create output. A simple stack that merely outputs a single
color orange tile looks like this:

    {"color" "#ff9900"}

Other layers in the current TileStache configuration can be reference by name,
as in this example stack that simply echoes another layer:

    {"src": "layer-name"}

Bitmap images can also be referenced by local filename or URL, and will be
tiled seamlessly, assuming 256x256 parent tiles:

    {"src": "image.png"}
    {"src": "http://example.com/image.png"}

Layers can be limited to appear at certain zoom levels, given either as a range
or as a single number:

    {"src": "layer-name", "zoom": "12"}
    {"src": "layer-name", "zoom": "12-18"}

Layers can also be used as masks, as in this example that uses one layer
to mask another layer:

    {"mask": "layer-name", "src": "other-layer"}

Many combinations of "src", "mask", and "color" can be used together, but it's
an error to provide all three.

Layers can be combined through the use of opacity and blend modes. Opacity is
specified as a value from 0.0-1.0, and blend mode is specified as a string.
This example layer is blended using the "hard light" mode at 50% opacity:

    {"src": "hillshading", "mode": "hard light", "opacity": 0.5}

Currently-supported blend modes include "screen", "add", "multiply", "subtract",
"linear light", and "hard light".

Layers can also be affected by adjustments. Adjustments are specified as an
array of names and parameters. This example layer has been slightly darkened
using the "curves" adjustment, moving the input value of 181 (light gray)
to 50% gray while leaving black and white alone:

    {"src": "hillshading", "adjustments": [ ["curves", [0, 181, 255]] ]}

Available adjustments:
  "threshold" - Blit.adjustments.threshold()
  "curves" - Blit.adjustments.curves()
  "curves2" - Blit.adjustments.curves2()

See detailed information about adjustments in Blit documentation:

    https://github.com/migurski/Blit#readme

Finally, the stacking feature allows layers to combined in more complex ways.
This example stack combines a background color and foreground layer:

    [
      {"color": "#ff9900"},
      {"src": "layer-name"}
    ]

A complete example configuration might look like this:

    {
      "cache":
      {
        "name": "Test"
      },
      "layers":
      {
        "base":
        {
          "provider": {"name": "mapnik", "mapfile": "mapnik-base.xml"}
        },
        "halos":
        {
          "provider": {"name": "mapnik", "mapfile": "mapnik-halos.xml"},
          "metatile": {"buffer": 128}
        },
        "outlines":
        {
          "provider": {"name": "mapnik", "mapfile": "mapnik-outlines.xml"},
          "metatile": {"buffer": 16}
        },
        "streets":
        {
          "provider": {"name": "mapnik", "mapfile": "mapnik-streets.xml"},
          "metatile": {"buffer": 128}
        },
        "sandwiches":
        {
          "provider":
          {
            "name": "Sandwich",
            "stack":
            [
              {"src": "base"},
              {"src": "outlines", "mask": "halos"},
              {"src": "streets"}
            ]
          }
        }
      }
    }
"""

from . import Core
from . import Sandwich

import multiprocessing, logging



class Provider(Sandwich.Provider):
    """ Sandwich Provider.

        Stack argument is a list of layer dictionaries described in module docs.
    """

    def draw_stack(self, coord, tiles):
        """ Render this image stack.

            Given a coordinate, return an output image with the results of all the
            layers in this stack pasted on in turn.

            Final argument is a dictionary used to temporarily cache results
            of layers retrieved from layer_bitmap(), to speed things up in case
            of repeatedly-used identical images.
        """
        stackLayers = tiles.keys()
        procs = []
        tileQueue = multiprocessing.Queue()

        for layer in self.stack:
            if 'zoom' in layer and not Sandwich.in_zoom(coord, layer['zoom']):
                continue

            source_name, mask_name, color_name = [layer.get(k, None) for k in ('src', 'mask', 'color')]

            if source_name and color_name and mask_name:
                raise Core.KnownUnknown("You can't specify src, color and mask together in a Sandwich Layer: %s, %s, %s" % (repr(source_name), repr(color_name), repr(mask_name)))

            #
            # For any layers we don't yet have, create a new process to
            # render that layer
            #

            if source_name and source_name not in stackLayers:
                # set a placeholder for this layer so we don't
                # build it more than once. It will be replaced
                # with the actual layer image later
                stackLayers.append(source_name)

                if source_name in self.config.layers:
                    logging.debug('TileStache.MPSandwich.draw_stack() adding layer_bitmap process %s/%d/%d/%d', source_name, coord.zoom, coord.column, coord.row)
                    procs.append(multiprocessing.Process(name=source_name, target=layer_bitmap, args=(source_name, self.config.layers[source_name], coord, tileQueue, )))
                else:
                    logging.debug('TileStache.MPSandwich.draw_stack() adding local_bitmap process %s/%d/%d/%d', source_name, coord.zoom, coord.column, coord.row)
                    procs.append(multiprocessing.Process(name=source_name, target=local_bitmap, args=(source_name, self.config, coord, self.layer.dim, tileQueue, )))

            if mask_name and mask_name not in stackLayers:
                logging.debug('TileStache.MPSandwich.draw_stack() adding layer_bitmap process %s/%d/%d/%d', mask_name, coord.zoom, coord.column, coord.row)
                stackLayers.append(mask_name)
                procs.append(multiprocessing.Process(name=mask_name, target=layer_bitmap, args=(mask_name, self.config.layers[mask_name], coord, tileQueue, )))


        numProcs = len(procs)
        logging.debug('TileStache.MPSandwich.draw_stack() using %d processes for %s/%d/%d/%d', numProcs, self.layer.name(), coord.zoom, coord.column, coord.row)

        # start all the processes
        for p in procs:
            logging.debug('TileStache.MPSandwich.draw_stack() starting process %s for %s/%d/%d/%d', p.name, self.layer.name(), coord.zoom, coord.column, coord.row)
            p.daemon = False
            p.start()

        # get the queue result for each process
        # MUST do this prior to joining processes
        for i in range(0, numProcs):
            # get will block until it can pull an item off the queue
            t = tileQueue.get()

            logging.debug('TileStache.MPSandwich.draw_stack() got queue result %s (%d of %d) for %s/%d/%d/%d', t.keys()[0], i+1, numProcs, self.layer.name(), coord.zoom, coord.column, coord.row)

            # update the tile dict with the rendered image
            tiles.update(t)

        logging.debug('TileStache.MPSandwich.draw_stack() closed queue for %s/%d/%d/%d', self.layer.name(), coord.zoom, coord.column, coord.row)
        tileQueue.close()

        # wait for all the processes to finish
        for p in procs: 
            p.join()
            logging.debug('TileStache.MPSandwich.draw_stack() joined process %s for %s/%d/%d/%d', p.name, self.layer.name(), coord.zoom, coord.column, coord.row)

        return Sandwich.Provider.draw_stack(self, coord, tiles);


def local_bitmap(source, config, coord, dim, q):
    q.put({source: Sandwich.local_bitmap(source, config, coord, dim)})


def layer_bitmap(source, layer, coord, q):
    q.put({source: Sandwich.layer_bitmap(layer, coord)})
