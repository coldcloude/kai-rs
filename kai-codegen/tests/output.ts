export type Address = {
  street: string;
  city: string;
  zip_code: string;
};

export type User = {
  id: number;
  name: string;
  email: string | null;
  age: number | null;
  address: Address;
};

export type Product = {
  product_id: string;
  price: number;
  in_stock: boolean;
  tags: string[];
  description: string | null;
  metadata: Record<string, string>;
};

export type Order = {
  order_id: string;
  user: User;
  products: Product[];
  total: number;
  status: string;
  discounts: Record<string, number> | null;
};

