use std::{os::unix::fs::MetadataExt, path::PathBuf};

use async_fuser::{AsyncSessionBuilder, Config, MountOption, SessionACL};
use tokio::{io::Result, sync::Mutex, task::JoinHandle};

use crate::port::FileSystemGateway;

use super::FuseFileSystem;

pub struct FuseMountHandle {
    handle: Mutex<Option<JoinHandle<Result<()>>>>,
    mountpoint: PathBuf,
    device_id: u64,
}

impl FuseMountHandle {
    pub async fn wait(&self) -> Result<()> {
        let Some(handle) = self.handle.lock().await.take() else {
            return Ok(());
        };
        handle
            .await
            .map_err(|err| std::io::Error::other(err.to_string()))?
    }

    pub async fn unmount(self) {
        let handle = self.handle.into_inner();
        let _ = std::process::Command::new("fusermount3")
            .args(["-u", "-q", "--"])
            .arg(&self.mountpoint)
            .status();

        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }

        let _ = std::fs::remove_dir(&self.mountpoint);
    }

    pub fn mounted(&self) -> bool {
        std::fs::metadata(&self.mountpoint)
            .map(|metadata| metadata.dev() == self.device_id)
            .unwrap_or(false)
    }
}

impl<T: FileSystemGateway + 'static> FuseFileSystem<T> {
    pub async fn mount(self, mountpoint: &str) -> Result<FuseMountHandle> {
        std::fs::create_dir_all(mountpoint)?;

        let mut config = Config::default();
        config.acl = SessionACL::RootAndOwner;
        config.mount_options.push(MountOption::AutoUnmount);
        let session = AsyncSessionBuilder::new()
            .filesystem(self)
            .mountpoint(mountpoint)
            .options(config)?
            .build()
            .await?;

        let handle = tokio::spawn(async move { session.run().await });
        let mountpoint = std::fs::canonicalize(mountpoint)?;
        let device_id = std::fs::metadata(&mountpoint)?.dev();

        Ok(FuseMountHandle {
            handle: Mutex::new(Some(handle)),
            mountpoint,
            device_id,
        })
    }
}
