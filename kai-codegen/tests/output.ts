export type User = {
  id: number;
  name: string;
  email: string | null;
  age: number | null;
};

export type Product = {
  product_id: string;
  price: number;
  in_stock: boolean;
  tags: string[];
  description: string | null;
};

