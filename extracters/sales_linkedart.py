from bonobo.config import use

from cromulent import model, vocab
from cromulent.model import factory
factory.auto_id_type = 'uuid'
vocab.add_art_setter()

def add_crom_data(data: dict, what=None):
	data['_CROM_FACTORY'] = factory
	data['_LOD_OBJECT'] = what
	return data

vocab.register_aat_class("Clock", {"parent": model.ManMadeObject, "id": "300041575", "label": "Clock"})
vocab.register_aat_class("Cards", {"parent": model.ManMadeObject, "id":"300211294", "label": "Playing Cards"})
object_type_map = {
	"Painting": vocab.Painting,
	"Drawing": vocab.Drawing,
	"Sculpture": vocab.Sculpture,
	"Photograph": vocab.Photograph,
	"Print": vocab.Print,
	"Book": vocab.Book,
	"Tapestry": vocab.Tapestry,
	"Decorative Art": vocab.DecArts,
	"Clocks": vocab.Clock,
	"Maps": vocab.Map,
	"Clothing": vocab.Clothing,
	"Playing Cards": vocab.Cards,
	"Furniture": vocab.Furniture
}
dimTypes = {300055624: vocab.Diameter, 300055644: vocab.Height, 300055647: vocab.Width}
dimUnits = {300379100: vocab.instances["inches"], 300379098: vocab.instances["cm"]}

# Here we take the data that has been collected and map it into Linked Art
# This should just be mapping, not data collection, manipulation or validation



