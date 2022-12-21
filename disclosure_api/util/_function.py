from datetime import timedelta
from decimal import Decimal
from datetime import date

def date_range(start, stop, step = timedelta(1)) -> date:
    current = start
    while current <= stop:
        yield current
        current += step

def decimal_normalize(f:Decimal):
    if not isinstance(f, str):
        def _remuve_exponent(d:Decimal):
            return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
        a = Decimal.normalize(Decimal(str(f)))
        b = _remuve_exponent(a)
        c = '{:,}'.format(b)
        return str(c)
    else:
        return '-'
