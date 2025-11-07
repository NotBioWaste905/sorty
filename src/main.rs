use blake3;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Small CLI:
/// sorty [PATH] [-r|--recursive]
///
/// - PATH: directory to scan (defaults to ".")
/// - -r / --recursive: traverse subdirectories
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (path, recursive) = parse_args()?;

    let start = Instant::now();

    let (files, empty_files) = collect_files(&path, recursive)?;
    if files.is_empty() {
        println!("No files to process.");
        if !empty_files.is_empty() {
            println!("\nEmpty files:");
            for p in empty_files {
                println!("  {}", p.display());
            }
        }
        return Ok(());
    }

    // First group by size to avoid hashing files of unique sizes
    let size_buckets = group_by_size(files);

    // Now hash only buckets where there are candidates (len > 1)
    let groups = group_by_hash(size_buckets)?;

    let duration = start.elapsed();

    print_report(&groups, &empty_files, duration);

    Ok(())
}

fn parse_args() -> Result<(PathBuf, bool), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let mut path = None;
    let mut recursive = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-r" | "--recursive" => recursive = true,
            "-h" | "--help" => {
                print_usage_and_exit();
            }
            _ => {
                if path.is_none() {
                    path = Some(PathBuf::from(arg));
                } else {
                    // ignore extra args for now
                }
            }
        }
    }

    let path = path.unwrap_or_else(|| PathBuf::from("."));
    if !path.exists() {
        return Err(format!("Path {:?} does not exist", path).into());
    }

    Ok((path, recursive))
}

fn print_usage_and_exit() -> ! {
    eprintln!("Usage: sorty [PATH] [-r|--recursive]");
    eprintln!("  PATH: directory to scan (defaults to \".\")");
    eprintln!("  -r, --recursive: traverse subdirectories");
    std::process::exit(1);
}

/// Traverse `path` and collect regular files.
/// If `recursive` is true, descend into directories recursively.
/// Returns (files, empty_files).
fn collect_files(path: &Path, recursive: bool) -> io::Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut files = Vec::new();
    let mut empty_files = Vec::new();
    if path.is_dir() {
        for entry_res in fs::read_dir(path)? {
            let entry = entry_res?;
            let p = entry.path();
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue, // skip unreadable entries
            };
            if meta.is_dir() {
                if recursive {
                    let (mut sub_files, mut sub_empty) = collect_files(&p, recursive)?;
                    files.append(&mut sub_files);
                    empty_files.append(&mut sub_empty);
                }
            } else if meta.is_file() {
                if meta.len() == 0 {
                    empty_files.push(p);
                } else {
                    files.push(p);
                }
            } else {
                // skip symlinks / other types
            }
        }
    } else if path.is_file() {
        let meta = fs::metadata(path)?;
        if meta.len() == 0 {
            empty_files.push(path.to_path_buf());
        } else {
            files.push(path.to_path_buf());
        }
    } else {
        // not a file or dir; nothing to do
    }

    Ok((files, empty_files))
}

/// Group files by their size (in bytes).
fn group_by_size(files: Vec<PathBuf>) -> HashMap<u64, Vec<PathBuf>> {
    let mut map: HashMap<u64, Vec<PathBuf>> = HashMap::new();
    for p in files {
        if let Ok(meta) = fs::metadata(&p) {
            let size = meta.len();
            map.entry(size).or_default().push(p);
        }
    }
    map
}

/// For each size bucket that has more than one file, compute blake3 hash (streamed)
/// and group by hash. Returns a Vec of groups (each group is Vec<PathBuf>) where len > 1.
fn group_by_hash(
    size_buckets: HashMap<u64, Vec<PathBuf>>,
) -> Result<Vec<Vec<PathBuf>>, Box<dyn std::error::Error>> {
    let mut groups: Vec<Vec<PathBuf>> = Vec::new();

    for (_size, bucket) in size_buckets.into_iter() {
        if bucket.len() <= 1 {
            continue; // unique size -> cannot be duplicate
        }

        // map hash -> files with that hash (within the same size)
        let mut hash_map: HashMap<blake3::Hash, Vec<PathBuf>> = HashMap::new();
        for p in bucket {
            match hash_file(&p) {
                Ok(h) => {
                    hash_map.entry(h).or_default().push(p);
                }
                Err(e) => {
                    eprintln!("Warning: failed to hash {}: {}", p.display(), e);
                    // skip unreadable file
                }
            }
        }

        for (_h, v) in hash_map {
            if v.len() > 1 {
                groups.push(v);
            }
        }
    }

    Ok(groups)
}

/// Stream-hash a file using a buffer to avoid loading it entirely into memory.
fn hash_file(path: &Path) -> io::Result<blake3::Hash> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

/// Print a human-friendly report:
/// - number of duplicate groups
/// - total duplicate files
/// - list groups with original + duplicates
/// - list empty files
/// - time elapsed
fn print_report(groups: &[Vec<PathBuf>], empty_files: &[PathBuf], duration: std::time::Duration) {
    let group_count = groups.len();
    let total_dup_files: usize = groups.iter().map(|g| g.len()).sum();

    println!("Report:");
    println!(
        "{} duplicate group(s), {} duplicate file(s) total",
        group_count, total_dup_files
    );

    for (i, group) in groups.iter().enumerate() {
        println!("\nGroup {} ({} files):", i + 1, group.len());
        for (j, p) in group.iter().enumerate() {
            if j == 0 {
                println!("  original:  {}", p.display());
            } else {
                println!("  duplicate: {}", p.display());
            }
        }
    }

    if empty_files.is_empty() {
        println!("\nNo empty files found.");
    } else {
        println!("\nEmpty files:");
        for p in empty_files {
            println!("  {}", p.display());
        }
    }

    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    println!("\nElapsed: {}.{:03} s", secs, millis);
}
