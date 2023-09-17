from dataclasses import dataclass, asdict
from typing import Optional, List, Dict


@dataclass
class TFWine:
    _similarity: Optional[float] = 0
    _name: Optional[str] = None
    _name_searchable: Optional[str] = None
    _name_extended: Optional[str] = None
    _country: Optional[str] = None
    _last_updated: Optional[str] = None
    _area: Optional[str] = None
    _categories: Optional[List[str]] = None
    _category: Optional[str] = None
    _type: Optional[str] = None
    _year: Optional[str] = None
    _brand: Optional[str] = None
    _price_NOK: Optional[int] = None
    _price_NOK_old: Optional[int] = None
    _price_NOK_litre: Optional[int] = None
    _price_NOK_litre_old: Optional[int] = None
    _price_EUR: Optional[int] = None
    _price_EUR_old: Optional[int] = None
    _price_EUR_litre: Optional[int] = None
    _price_EUR_litre_old: Optional[int] = None
    _unit: Optional[str] = None
    _amount: Optional[float] = None
    _description: Optional[str] = None
    _alc_percentage: Optional[str] = None
    _grapes: Optional[str] = None
    _url: Optional[str] = None

    _availability: Optional[List[str]] = None
    _availability_airports: Optional[List[str]] = None
    _in_stock_in: Optional[List[str]] = None

    def update_price(self, updates):
        # TODO: Test this! 
        # for param_name, new_value in updates.items():
        #     if hasattr(self, param_name):
        #         setattr(self, param_name, new_value)
        #     else:
        #         print(f"Invalid parameter name: {param_name}")
        for param_name, new_value in updates.items():
            if "NOK" in param_name:
                if self._price_NOK == None:
                    self._price_NOK = new_value
                    self._price_NOK_old = new_value
                else:
                    self._price_NOK = new_value

                if self._amount != None:
                    self._price_NOK_litre = float(self._price_NOK/self._amount)
                    self._price_NOK_litre_old = float(self._price_NOK_old/self._amount)
                
            elif "EUR" in param_name:
                if self._price_EUR == None:
                    self._price_EUR = new_value
                    self._price_EUR_old = new_value
                else:
                    self._price_EUR = new_value

                if self._amount != None:
                    self._price_EUR_litre = float(self._price_EUR/self._amount)
                    self._price_EUR_litre_old = float(self._price_EUR_old/self._amount)
            
            else:
                print(f"Invalid parameter name: {param_name} for wine: {self._name}. Must be EUR or NOK")

    def to_dict(self):
        return asdict(self)

@dataclass
class VVWine:
    _similarity: Optional[float] = 0
    _name: Optional[str] = None
    _last_updated: Optional[str] = None
    _match_percentage: Optional[int] = None
    _rating: Optional[int] = None
    _no_of_ratings: Optional[int] = None
    _wine_url: Optional[str] = None
    _full_wine_url: Optional[str] = None
    _image_url: Optional[str] = None

    def to_dict(self):
        return asdict(self)

@dataclass
class PolWine:
    _similarity: Optional[float] = 0
    _name: Optional[str] = None
    _name_searchable: Optional[str] = None
    _code: Optional[str] = None
    _name_extended: Optional[str] = None
    _last_updated: Optional[str] = None
    _main_country: Optional[str] = None
    _district: Optional[str] = None
    _main_category: Optional[str] = None
    _main_sub_category: Optional[str] = None
    _price: Optional[float] = None
    _price_old: Optional[float] = None
    _volume: Optional[float] = None
    _price_litre: Optional[str] = None
    _price_litre_old: Optional[str] = None
    _expired: Optional[bool] = None
    _buyable: Optional[bool] = None
    _url: Optional[str] = None

    _availability_text: Optional[str] = None
    _local_bargain: Optional[bool] = False #What was the idea behind this one? TO NOT use the product code in the name and then add it into local bargain if different? 

    def set_availability(self, avail_text):
        if avail_text == 'Kan bestilles til alle butikker':
            self._availability_text = avail_text
            self._local_bargain = False
        elif avail_text == 'Kan kjøpes i butikk med varen på lager':
            self._availability_text = avail_text
            self._local_bargain = True
        else:
            print(f"Availability not defined for {self._name}")

    def update_price(self, new_value):
        if new_value != None:
            if not isinstance(new_value, (int, float)):
                print("Value is not a valid numeric type.")
            else:
                if self._price == None:
                    self._price = new_value
                    self._price_old = new_value
                else:
                    self._price = new_value

                if self._volume != None:
                    self._price_litre = float(self._price/self._volume)
                    self._price_litre_old = float(self._price_old/self._volume)

    def to_dict(self):
        return asdict(self)
        


@dataclass
class Wine:
    # TODO: Find a good way to search though the dictionaries of the closest match wrt name. Pandas? Vector database? Similarity searches? Difflib? 
    name: Optional[str] = None
    last_updated: Optional[str] = None

    # Internal objects
    taxfree_wine: Optional[TFWine] = None
    polet_wine: Optional[Dict[str,PolWine]] = None
    vivino_wine: Optional[VVWine] = None


    def to_dict(self):
        json_dict = asdict(self)
        try:
            json_dict["pol_wines"] = {
                key: pol_wine.to_dict() for key, pol_wine in self.polet_wine.items()
            }
        except AttributeError:
            print("No wines from Polet found, return None")
        return json_dict




