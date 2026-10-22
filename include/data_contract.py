"""CSV schemas and the INR half-up, two-decimal currency policy."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

SCHEMAS = {
    'orders': 'order_id order_ts customer_id store_id product_id quantity unit_price discount_pct order_amount payment_mode order_status channel delivery_minutes'.split(),
    'payments': 'payment_id order_id payment_ts payment_mode amount_paid payment_status gateway_ref'.split(),
    'customers': 'customer_id full_name email phone city area pincode signup_date loyalty_tier is_active'.split(),
    'products': 'product_id product_name category sub_category brand mrp cost_price unit is_perishable launch_date'.split(),
    'stores': 'store_id store_name zone area pincode open_date manager_name capacity_orders_per_day'.split(),
}
KEYS = dict(orders='order_id', payments='payment_id', customers='customer_id', products='product_id', stores='store_id')
MODES = {'UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET'}

def money(value):
    try:
        d = Decimal(str(value))
        return d.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if d.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None

def order_money(quantity, price, discount):
    try:
        return money(Decimal(str(quantity)) * Decimal(str(price)) * (1 - Decimal(str(discount)) / 100))
    except (InvalidOperation, ValueError, TypeError):
        return None

def check_schema(source, columns):
    missing = sorted(set(SCHEMAS[source]) - set(columns))
    extra = sorted(set(columns) - set(SCHEMAS[source]))
    if missing or extra or len(columns) != len(set(columns)):
        raise ValueError(f"{source}: missing required columns: {', '.join(missing)}; unexpected columns: {', '.join(extra)}")
