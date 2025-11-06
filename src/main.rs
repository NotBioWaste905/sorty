use blake3;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::Error;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Error> {
    let path = Path::new("/home/askatasuna/Загрузки");
    let report = find_duplicates(path)?;
    // Use Display impls instead of Debug
    println!("{}", report);
    Ok(())
}

struct Duplicates {
    num: u16,
    file_paths: Vec<PathBuf>,
}

struct Report {
    duplicates: Duplicates,
    empty_files: Vec<PathBuf>,
}

impl fmt::Display for Duplicates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Found {} duplicate file(s):", self.num)?;
        for p in &self.file_paths {
            writeln!(f, "  {}", p.display())?;
        }
        Ok(())
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Report:")?;
        writeln!(f, "{}", self.duplicates)?;
        if self.empty_files.is_empty() {
            writeln!(f, "No empty files found.")?;
        } else {
            writeln!(f, "Empty files:")?;
            for p in &self.empty_files {
                writeln!(f, "  {}", p.display())?;
            }
        }
        Ok(())
    }
}

fn find_duplicates(path: &Path) -> Result<Report, Error> {
    let mut num: u16 = 0;
    let mut empty_files: Vec<PathBuf> = Vec::new();
    let mut files_w_same_sizes: HashMap<blake3::Hash, Vec<PathBuf>> = HashMap::new();
    let mut file_paths: Vec<PathBuf> = Vec::new();

    if path.is_dir() {
        for entry_res in fs::read_dir(path)? {
            let entry = entry_res?;
            let entry_path = entry.path();
            if entry_path.is_file() {
                let bytes = fs::read(&entry_path)?;
                if bytes.is_empty() {
                    empty_files.push(entry_path.clone());
                    continue;
                }
                // let filesize = bytes.len();
                let hash = blake3::hash(&bytes);
                // add to hash bucket; if bucket already has items, we found a duplicate file
                let bucket = files_w_same_sizes.entry(hash).or_insert_with(Vec::new);
                if !bucket.is_empty() {
                    // this file is a duplicate of at least one earlier file
                    num += 1;
                }
                bucket.push(entry_path.clone());
            }
        }

        // let mut duplicate_files: HashMap<blake3::Hash, Vec<PathBuf>> = HashMap::new();

        for vector in files_w_same_sizes.values() {
            if vector.len() > 1 {
                // let hash = blake3::hash(&bytes);
                // extend file_paths with the entries in this bucket
                file_paths.extend_from_slice(&vector);
            }
        }
    } else {
        println!("The path {path:?} is not a directory!");
    }

    Ok(Report {
        duplicates: Duplicates { num, file_paths },
        empty_files,
    })
}
