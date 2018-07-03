package cvmfs

import (
	"context"
	"fmt"
	//"golang.org/x/sys/unix"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	logg "github.com/sirupsen/logrus"

	"github.com/cvmfs/docker-graphdriver/plugins/util"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.SnapshotPlugin,
		ID:   "cvmfs",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())
			return NewSnapshotter(ic.Root)
		},
	})
}

type snapshotter struct {
	root         string
	ms           *storage.MetaStore
	cvmfsManager util.ICvmfsManager
	mountedDir   []string
}

func NewSnapshotter(root string) (snapshots.Snapshotter, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "new"}).Info()
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return &snapshotter{
		root:       root,
		ms:         ms,
		mountedDir: make([]string, 12),
	}, nil
}

func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "stat",
		"key":         key}).Info()
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()
	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "update"}).Info()

	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "usage",
		"key":         key}).Info()
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}
	defer t.Rollback()

	id, info, usage, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		du, err := fs.DiskUsage(o.getSnapshotDir(id))
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "prepare",
		"key":         key,
		"parent":      parent}).Info()
	var (
		err      error
		path, td string
	)

	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}

	s, err := storage.CreateSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
	if err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
		}
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	fmt.Println("Got snapshot")

	path = o.getSnapshotDir(s.ID)
	os.Mkdir(path, 0600)

	fmt.Println("Got path: ", path)
	logg.WithFields(logg.Fields{
		"td":     td,
		"parent": parent,
		"path":   path,
	}).Info()

	if err := t.Commit(); err != nil {
		fmt.Println("9.0")
		return nil, errors.Wrap(err, "commit failed")
	}
	fmt.Println("10.0")
	return o.mounts(s), nil
}

/*
func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "prepare",
		"key":         key,
		"parent":      parent}).Info()
	var (
		err      error
		path, td string
	)

	td, err = ioutil.TempDir(filepath.Join(o.root, "snapshots"), "new-")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create temp dir")
	}
	defer func() {
		logg.WithFields(logg.Fields{
			"td":   td,
			"path": path,
		}).Info("Inside defer")
		// if there was some error just clean up everything...
		if err != nil {
			if td != "" {
				if err1 := os.RemoveAll(td); err1 != nil {
					err = errors.Wrapf(err, "remove failed: %v", err1)
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					err = errors.Wrapf(err, "failed to remove path: %v", err1)
				}
			}
		}
	}()

	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}

	s, err := storage.CreateSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
	if err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
		}
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	fmt.Println("Got snapshot")

	if td != "" {
		path = o.getSnapshotDir(s.ID)
		os.Mkdir(path, 0600)
		if len(s.ParentIDs) > 0 {
			fmt.Println("1.0")
			parent := o.getSnapshotDir(s.ParentIDs[0])
			fmt.Println("2.0")

			// use bind also here
			if err := unix.Mount(parent, path, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
				// if err := fs.CopyDir(td, parent); err != nil {
				return nil, errors.Wrap(err, "mounting of parent failed")
			}
			fmt.Println("3.0")
			fmt.Println("Got parent: ", parent)
		}

		fmt.Println("4.0")
		fmt.Println("5.0")
		// here instead of renaming we can mount --bind the directory and see what happens
		// if err := os.Rename(td, path); err != nil {
		//fmt.Println("td :>", td, "\npath :>", path)
		if err := unix.Mount(path, td, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			fmt.Println("Err 6.0")
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
			fmt.Println("Err 7.0")
			return nil, errors.Wrap(err, "failed to mount bind")
		}
		fmt.Println("Got path: ", path)
		logg.WithFields(logg.Fields{
			"td":     td,
			"parent": parent,
			"path":   path,
		}).Info()
		td = ""
	}

	fmt.Println("8.0")

	if err := t.Commit(); err != nil {
		fmt.Println("9.0")
		return nil, errors.Wrap(err, "commit failed")
	}
	fmt.Println("10.0")
	return o.mounts(s), nil
}
*/
func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "view",
		"key":         key,
		"parent":      parent}).Info()
	return nil, nil
}

func (o *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "mounts",
		"key":         key}).Info()
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}
	s, err := storage.GetSnapshot(ctx, key)
	t.Rollback()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot mount")
	}
	return o.mounts(s), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "commit",
		"key":         key,
		"name":        name}).Info()
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}

	id, si, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	fmt.Println(si)

	files, err := ioutil.ReadDir(o.getSnapshotDir(id))
	if err != nil {
		logg.WithFields(logg.Fields{
			"snapshotter": "cvmfs",
			"action":      "Commit",
			"source":      o.getSnapshotDir(id),
			"err":         err}).Info()
	}

	fmt.Println("Start printing file")
	for _, p := range files {
		if p.Name() == "thin.json" {
			var paths_to_mount []string
			f, err := ioutil.ReadFile(o.getSnapshotDir(id) + "/" + p.Name())
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(string(f))
			t := util.ReadThinFile(o.getSnapshotDir(id) + "/" + p.Name())
			for _, layer := range t.Layers {
				protocol, path := util.ParseThinUrl(layer.Url)
				to_mount := "/" + protocol + "/" + path
				paths_to_mount = append(paths_to_mount, to_mount)
			}
			lower_to_mounts_formated := strings.Join(paths_to_mount, ":")

			//mount_command := fmt.Sprintf("mount --verbose -t overlay overlay -o lowerdir=%s %s", lower_to_mounts_formated, o.getSnapshotDir(id))
			os.Mkdir(o.getSnapshotDir(id), 0655)
			fmt.Println("dir: ", o.getSnapshotDir(id))
			loweroptions := fmt.Sprintf("-olowerdir=%s", lower_to_mounts_formated)

			//mount_command := fmt.Sprintf("mount -v -t overlay overlay -olowerdir=%s %s",
			//	lower_to_mounts_formated, o.getSnapshotDir(id))

			//fmt.Println(mount_command)

			cmd := exec.Command("mount", "-v", "-t", "overlay", "overlay", loweroptions, o.getSnapshotDir(id))
			/*
				stdoutStderr, err := cmd.CombinedOutput()
				if err != nil {
					fmt.Println("Error in creating the stdout pipes")
				}
			*/
			err = cmd.Start()
			fmt.Println("Error in mount: ", err)
			err = cmd.Wait()
			fmt.Println("Error in mount: ", err)
			o.mountedDir = append(o.mountedDir, o.getSnapshotDir(id))

		}
	}
	fmt.Println("Finish printing files")

	usage, err := fs.DiskUsage(o.getSnapshotDir(id))
	if err != nil {
		return err
	}

	if _, err := storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		if rerr := t.Rollback(); rerr != nil {
			log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
		}
		return errors.Wrap(err, "failed to commit snapshot")
	}
	return t.Commit()
}

func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "remove",
		"key":         key}).Info()
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && t != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	id, _, err := storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	path := o.getSnapshotDir(id)
	renamed := filepath.Join(o.root, "snapshots", "rm-"+id)
	if err := os.Rename(path, renamed); err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, "failed to rename")
		}
		renamed = ""
	}

	err = t.Commit()
	t = nil
	if err != nil {
		if renamed != "" {
			if err1 := os.Rename(renamed, path); err1 != nil {
				// May cause inconsistent data on disk
				log.G(ctx).WithError(err1).WithField("path", renamed).Errorf("failed to rename after failed commit")
			}
		}
		return errors.Wrap(err, "failed to commit")
	}
	if renamed != "" {
		if err := os.RemoveAll(renamed); err != nil {
			// Must be cleaned up, any "rm-*" could be removed if no active transactions
			log.G(ctx).WithError(err).WithField("path", renamed).Warnf("failed to remove root filesystem")
		}
	}

	return nil
}

func (o *snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error) error {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "walk"}).Info()
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return storage.WalkInfo(ctx, fn)
}

func (o *snapshotter) Close() error {
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "close"}).Info()

	for i := range o.mountedDir {
		cmd := exec.Command("umount", o.mountedDir[i])
		cmd.Start()
		cmd.Wait()
	}
	return o.ms.Close()
}

func (o *snapshotter) getSnapshotDir(id string) string {
	return filepath.Join(o.root, "snapshots", id)
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	var (
		roFlag string
		source string
	)

	if s.Kind == snapshots.KindView {
		roFlag = "ro"
	} else {
		roFlag = "rw"
	}

	if len(s.ParentIDs) == 0 {
		fmt.Println("Option A")
		source = o.getSnapshotDir(s.ID)
	} else {
		fmt.Println("Option B")
		source = o.getSnapshotDir(s.ParentIDs[0])
	}
	logg.WithFields(logg.Fields{
		"snapshotter": "cvmfs",
		"action":      "snapshotter.mounts",
		"source":      source}).Info()

	files, err := ioutil.ReadDir(source)
	if err != nil {
		logg.WithFields(logg.Fields{
			"snapshotter": "cvmfs",
			"action":      "snapshotter.mounts.readdir",
			"source":      source,
			"err":         err}).Info()
	}

	fmt.Println("Start printing file")
	for _, f := range files {
		fmt.Println(f.Name())
	}
	fmt.Println("Finish printing files")

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.getSnapshotDir(s.ParentIDs[i])
	}

	fmt.Println(parentPaths)

	toReturn := []mount.Mount{
		{
			Source: source,
			Type:   "bind",
			Options: []string{
				roFlag,
				"rbind",
			},
		},
	}
	fmt.Println(toReturn)
	return toReturn
}
