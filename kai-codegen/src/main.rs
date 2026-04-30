use std::collections::HashMap;
use std::fs;
use std::env;
use thiserror::Error;
use syn::parse_file;
use syn::{Data, DataStruct, Fields, FieldsNamed, Path, Type, TypePath};

#[derive(Error, Debug)]
enum CodegenError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Parse error: {0}")]
    ParseError(String),
}

#[derive(Debug, Clone)]
struct StructField {
    name: String,
    rust_type: String,
}

#[derive(Debug, Clone)]
struct StructDef {
    name: String,
    fields: Vec<StructField>,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 3 {
        eprintln!("Usage: {} <input.rs> <output.ts>", args[0]);
        std::process::exit(1);
    }
    
    let input_path = &args[1];
    let output_path = &args[2];
    
    match convert_rust_to_ts(input_path, output_path) {
        Ok(_) => println!("Successfully generated TypeScript definitions at {}", output_path),
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

fn convert_rust_to_ts(input_path: &str, output_path: &str) -> Result<(), CodegenError> {
    let content = fs::read_to_string(input_path)?;
    let structs = parse_rust_file(&content)?;
    let ts_code = generate_typescript(&structs);
    fs::write(output_path, ts_code)?;
    Ok(())
}

fn parse_rust_file(content: &str) -> Result<Vec<StructDef>, CodegenError> {
    let file = parse_file(content).map_err(|e| CodegenError::ParseError(e.to_string()))?;
    
    let mut structs = Vec::new();
    
    for item in file.items {
        if let syn::Item::Struct(item_struct) = item {
            let struct_def = parse_struct(item_struct)?;
            structs.push(struct_def);
        }
    }
    
    Ok(structs)
}

fn parse_struct(item_struct: syn::ItemStruct) -> Result<StructDef, CodegenError> {
    let name = item_struct.ident.to_string();
    
    let fields = match item_struct.fields {
        Fields::Named(FieldsNamed { named, .. }) => {
            named
                .into_iter()
                .map(|field| {
                    let field_name = field.ident.unwrap().to_string();
                    let rust_type = type_to_string(&field.ty);
                    Ok(StructField {
                        name: field_name,
                        rust_type,
                    })
                })
                .collect::<Result<Vec<_>, CodegenError>>()?
        }
        Fields::Unit | Fields::Unnamed(_) => Vec::new(),
    };
    
    Ok(StructDef { name, fields })
}

fn type_to_string(ty: &Type) -> String {
    match ty {
        Type::Path(TypePath { path, .. }) => path_to_string(path),
        Type::Reference(r) => {
            format!("&{}", type_to_string(&r.elem))
        }
        Type::Paren(p) => format!("({})", type_to_string(&p.elem)),
        _ => "any".to_string(),
    }
}

fn path_to_string(path: &Path) -> String {
    let segments: Vec<String> = path
        .segments
        .iter()
        .map(|seg| seg.ident.to_string())
        .collect();
    
    if segments.len() == 1 {
        let seg = &path.segments.first().unwrap();
        match &seg.arguments {
            syn::PathArguments::AngleBracketed(args) => {
                let args_str: Vec<String> = args
                    .args
                    .iter()
                    .filter_map(|arg| match arg {
                        syn::GenericArgument::Type(ty) => Some(type_to_string(ty)),
                        _ => None,
                    })
                    .collect();
                format!("{}<{}>", segments.join("::"), args_str.join(", "))
            }
            syn::PathArguments::None => segments.join("::"),
            _ => segments.join("::"),
        }
    } else {
        segments.join("::")
    }
}

fn generate_typescript(structs: &[StructDef]) -> String {
    let mut output = String::new();
    
    for struct_def in structs {
        output.push_str(&format!("export type {} = {{\n", struct_def.name));
        
        for field in &struct_def.fields {
            let ts_type = rust_type_to_ts(&field.rust_type);
            output.push_str(&format!("  {}: {};\n", field.name, ts_type));
        }
        
        output.push_str("};\n\n");
    }
    
    output
}

fn rust_type_to_ts(rust_type: &str) -> String {
    let mut type_map = HashMap::new();
    type_map.insert("i8", "number");
    type_map.insert("i16", "number");
    type_map.insert("i32", "number");
    type_map.insert("i64", "number");
    type_map.insert("isize", "number");
    type_map.insert("u8", "number");
    type_map.insert("u16", "number");
    type_map.insert("u32", "number");
    type_map.insert("u64", "number");
    type_map.insert("usize", "number");
    type_map.insert("f32", "number");
    type_map.insert("f64", "number");
    type_map.insert("bool", "boolean");
    type_map.insert("String", "string");
    type_map.insert("&str", "string");
    type_map.insert("char", "string");
    
    if rust_type.starts_with("Option<") && rust_type.ends_with('>') {
        let inner = &rust_type[7..rust_type.len() - 1];
        return format!("{} | null", rust_type_to_ts(inner));
    }
    
    if rust_type.starts_with("Vec<") && rust_type.ends_with('>') {
        let inner = &rust_type[4..rust_type.len() - 1];
        return format!("{}[]", rust_type_to_ts(inner));
    }
    
    if rust_type.starts_with("HashMap<") && rust_type.ends_with('>') {
        let inner = &rust_type[8..rust_type.len() - 1];
        if let Some(pos) = inner.find(", ") {
            let key_type = rust_type_to_ts(&inner[0..pos]);
            let value_type = rust_type_to_ts(&inner[pos+2..]);
            return format!("Record<{}, {}>", key_type, value_type);
        }
        if let Some(pos) = inner.find(',') {
            let key_type = rust_type_to_ts(&inner[0..pos]);
            let value_type = rust_type_to_ts(&inner[pos+1..].trim());
            return format!("Record<{}, {}>", key_type, value_type);
        }
    }
    
    if let Some(&ts_type) = type_map.get(rust_type) {
        return ts_type.to_string();
    }
    
    rust_type.to_string()
}
