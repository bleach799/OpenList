package quark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	streamPkg "github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/go-resty/resty/v2"
)

var (
	_ driver.PutResult    = (*QuarkOrUC)(nil)
	_ driver.RenameResult = (*QuarkOrUC)(nil)
	_ driver.MoveResult   = (*QuarkOrUC)(nil)
	_ driver.MkdirResult  = (*QuarkOrUC)(nil)
)

// fakeQuark serves the clouddrive endpoints used by the driver. While it lags,
// /file/sort keeps answering with the listings from before, the way Quark does
// for a few seconds after a change.
type fakeQuark struct {
	mu       sync.Mutex
	files    map[string]File   // by fid
	parents  map[string]string // fid -> parent fid
	stale    map[string][]File // parent fid -> listing served while lagging
	lagging  bool
	uploads  map[string]string // task id -> fid
	pending  map[string]File   // fid -> file being uploaded
	nextID   int
	noFid    bool
	renamed  []string
	deleted  []string
	mkdirErr string
}

func newFakeQuark(t *testing.T) (*fakeQuark, *QuarkOrUC) {
	t.Helper()
	q := &fakeQuark{
		files:   map[string]File{},
		parents: map[string]string{},
		uploads: map[string]string{},
		pending: map[string]File{},
	}
	srv := httptest.NewServer(http.HandlerFunc(q.serve))
	t.Cleanup(srv.Close)

	prev := base.RestyClient
	base.RestyClient = resty.New().SetTimeout(5 * time.Second)
	t.Cleanup(func() { base.RestyClient = prev })

	d := &QuarkOrUC{
		Storage:  model.Storage{MountPath: "/" + t.Name(), CacheExpiration: 30},
		Addition: Addition{RootID: driver.RootID{RootFolderID: "0"}, OrderBy: "none"},
		config:   driver.Config{Name: "Quark", DefaultRoot: "0", NoOverwriteUpload: true},
		conf:     Conf{api: srv.URL + "/1/clouddrive"},
	}
	return q, d
}

func (q *fakeQuark) add(parent string, f File) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.files[f.Fid] = f
	q.parents[f.Fid] = parent
}

func (q *fakeQuark) set(fn func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	fn()
}

// calls returns the fids that have been renamed and deleted so far.
func (q *fakeQuark) calls() (renamed, deleted []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.renamed), slices.Clone(q.deleted)
}

// lag freezes every listing until catchUp is called.
func (q *fakeQuark) lag() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stale = map[string][]File{"0": q.current("0")}
	for _, parent := range q.parents {
		q.stale[parent] = q.current(parent)
	}
	q.lagging = true
}

func (q *fakeQuark) catchUp() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lagging = false
}

func (q *fakeQuark) current(parent string) []File {
	list := []File{}
	for fid, f := range q.files {
		if q.parents[fid] == parent {
			list = append(list, f)
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Fid < list[j].Fid })
	return list
}

func (q *fakeQuark) serve(w http.ResponseWriter, r *http.Request) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var req struct {
		Fid       string   `json:"fid"`
		FileName  string   `json:"file_name"`
		PdirFid   string   `json:"pdir_fid"`
		Size      int64    `json:"size"`
		TaskID    string   `json:"task_id"`
		FileList  []string `json:"filelist"`
		ToPdirFid string   `json:"to_pdir_fid"`
	}
	if r.Method == http.MethodPost {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	var data any
	var metadata any
	switch r.URL.Path {
	case "/1/clouddrive/file/sort":
		parent := r.URL.Query().Get("pdir_fid")
		list := q.current(parent)
		if q.lagging {
			list = q.stale[parent]
		}
		data = map[string]any{"list": list}
		metadata = map[string]any{"_total": len(list)}
	case "/1/clouddrive/file/upload/pre":
		q.nextID++
		fid, task := fmt.Sprintf("new-%d", q.nextID), fmt.Sprintf("task-%d", q.nextID)
		q.uploads[task] = fid
		q.pending[fid] = File{Fid: fid, FileName: req.FileName, Size: req.Size, File: true}
		q.parents[fid] = req.PdirFid
		if q.noFid {
			fid = ""
		}
		data = map[string]any{"task_id": task, "fid": fid, "upload_id": "upload", "obj_key": "obj", "bucket": "bucket"}
		metadata = map[string]any{"part_size": 4 * utils.MB}
	case "/1/clouddrive/file/update/hash":
		// every upload in these tests is a rapid upload
		fid := q.uploads[req.TaskID]
		q.files[fid] = q.pending[fid]
		data = map[string]any{"finish": true}
	case "/1/clouddrive/file/rename":
		f := q.files[req.Fid]
		f.FileName = req.FileName
		q.files[req.Fid] = f
		q.renamed = append(q.renamed, req.Fid)
	case "/1/clouddrive/file/delete":
		for _, fid := range req.FileList {
			delete(q.files, fid)
			q.deleted = append(q.deleted, fid)
		}
	case "/1/clouddrive/file/move":
		for _, fid := range req.FileList {
			q.parents[fid] = req.ToPdirFid
		}
	case "/1/clouddrive/file":
		if q.mkdirErr != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": 400, "code": 1, "message": q.mkdirErr})
			return
		}
		q.nextID++
		fid := fmt.Sprintf("dir-%d", q.nextID)
		q.files[fid] = File{Fid: fid, FileName: req.FileName}
		q.parents[fid] = req.PdirFid
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "code": 0, "data": data, "metadata": metadata})
}

func newStream(name, content string) *streamPkg.FileStream {
	payload := []byte(content)
	return &streamPkg.FileStream{
		Obj: &model.Object{
			Name: name,
			Size: int64(len(payload)),
			HashInfo: utils.NewHashInfoByMap(map[*utils.HashType]string{
				utils.MD5:  utils.HashData(utils.MD5, payload),
				utils.SHA1: utils.HashData(utils.SHA1, payload),
			}),
		},
		Reader:   bytes.NewReader(payload),
		Mimetype: "application/octet-stream",
	}
}

func ids(objs []model.Obj) []string {
	res := make([]string, 0, len(objs))
	for _, obj := range objs {
		res = append(res, obj.GetID())
	}
	slices.Sort(res)
	return res
}

// Overwriting a file renames the old one, uploads the new one and deletes the
// old one. None of that shows up in /file/sort right away, which used to leave
// the new file out of the cache (WebDAV clients got 404 and uploaded again) and
// the old one undeleted.
func TestOverwriteWhileListingLags(t *testing.T) {
	q, d := newFakeQuark(t)
	q.add("0", File{Fid: "old", FileName: "complete_list.db", Size: 2, File: true})
	ctx := context.Background()

	if obj, err := op.Get(ctx, d, "/complete_list.db"); err != nil || obj.GetID() != "old" {
		t.Fatalf("get before upload: %v, %v", obj, err)
	}
	q.lag()

	if err := op.Put(ctx, d, "/", newStream("complete_list.db", "v2"), nil); err != nil {
		t.Fatalf("first overwrite: %v", err)
	}
	if _, deleted := q.calls(); !slices.Equal(deleted, []string{"old"}) {
		t.Fatalf("deleted %v, want the replaced file [old]", deleted)
	}
	if obj, err := op.Get(ctx, d, "/complete_list.db"); err != nil || obj.GetID() != "new-1" {
		t.Fatalf("get after upload: %v, %v", obj, err)
	}

	// a refresh, e.g. after the cache expired, while Quark still lags
	objs, err := op.List(ctx, d, "/", model.ListArgs{Refresh: true})
	if err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := ids(objs); !slices.Equal(got, []string{"new-1"}) {
		t.Fatalf("listing after refresh %v, want [new-1]", got)
	}

	if err := op.Put(ctx, d, "/", newStream("complete_list.db", "v3"), nil); err != nil {
		t.Fatalf("second overwrite: %v", err)
	}
	renamed, deleted := q.calls()
	if !slices.Equal(renamed, []string{"old", "new-1"}) || !slices.Equal(deleted, []string{"old", "new-1"}) {
		t.Fatalf("renamed %v, deleted %v: the second overwrite must replace new-1", renamed, deleted)
	}

	q.catchUp()
	objs, err = op.List(ctx, d, "/", model.ListArgs{Refresh: true})
	if err != nil {
		t.Fatalf("refresh after catching up: %v", err)
	}
	if got := ids(objs); !slices.Equal(got, []string{"new-2"}) {
		t.Fatalf("listing after catching up %v, want [new-2]", got)
	}
}

// A file uploaded into a directory that is not cached yet must still be found
// right after the upload, e.g. by the PROPFIND a WebDAV client sends next.
func TestNewFileFoundWhileListingLags(t *testing.T) {
	q, d := newFakeQuark(t)
	q.add("0", File{Fid: "other", FileName: "other.index", Size: 1, File: true})
	q.lag()
	ctx := context.Background()

	if err := op.Put(ctx, d, "/", newStream("20.lock", "lock"), nil); err != nil {
		t.Fatalf("upload: %v", err)
	}
	op.Cache.DeleteDirectory(d, "/")
	if obj, err := op.Get(ctx, d, "/20.lock"); err != nil || obj.GetID() != "new-1" {
		t.Fatalf("get after upload: %v, %v", obj, err)
	}
	if err := op.Remove(ctx, d, "/20.lock"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, deleted := q.calls(); !slices.Equal(deleted, []string{"new-1"}) {
		t.Fatalf("deleted %v, want [new-1]", deleted)
	}
}

func TestPutWithoutFid(t *testing.T) {
	q, d := newFakeQuark(t)
	q.set(func() { q.noFid = true })
	root := &model.Object{ID: "0", IsFolder: true}
	obj, err := d.Put(context.Background(), root, newStream("a.txt", "a"), func(float64) {})
	if err != nil || obj != nil {
		t.Fatalf("Put = %v, %v; want no object so op.Put falls back to a temp entry", obj, err)
	}
}

func TestMoveWhileListingLags(t *testing.T) {
	q, d := newFakeQuark(t)
	q.add("0", File{Fid: "dir", FileName: "dir"})
	q.add("0", File{Fid: "f", FileName: "f.index", Size: 1, File: true})
	ctx := context.Background()
	if _, err := op.List(ctx, d, "/", model.ListArgs{}); err != nil {
		t.Fatal(err)
	}
	q.lag()

	if err := op.Move(ctx, d, "/f.index", "/dir"); err != nil {
		t.Fatalf("move: %v", err)
	}
	src, err := op.List(ctx, d, "/", model.ListArgs{Refresh: true})
	if err != nil {
		t.Fatal(err)
	}
	dst, err := op.List(ctx, d, "/dir", model.ListArgs{Refresh: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ids(src), []string{"dir"}; !slices.Equal(got, want) {
		t.Fatalf("source listing %v, want %v", got, want)
	}
	if got, want := ids(dst), []string{"f"}; !slices.Equal(got, want) {
		t.Fatalf("target listing %v, want %v", got, want)
	}
}

func TestMakeDirWaitsForListing(t *testing.T) {
	q, d := newFakeQuark(t)
	q.lag()
	time.AfterFunc(300*time.Millisecond, q.catchUp)
	root := &model.Object{ID: "0", IsFolder: true}

	dir, err := d.MakeDir(context.Background(), root, "Pool")
	if err != nil || dir == nil {
		t.Fatalf("MakeDir = %v, %v; want the listed directory", dir, err)
	}
	if !dir.IsDir() || dir.GetName() != "Pool" || dir.GetID() != "dir-1" {
		t.Fatalf("MakeDir returned %+v", dir)
	}
}

func TestMakeDirNotListedInTime(t *testing.T) {
	q, d := newFakeQuark(t)
	q.lag()
	root := &model.Object{ID: "0", IsFolder: true}

	dir, err := d.MakeDir(context.Background(), root, "Pool")
	if err != nil || dir != nil {
		t.Fatalf("MakeDir = %v, %v; want no object so op.MakeDir falls back to a temp entry", dir, err)
	}

	q.set(func() { q.mkdirErr = "file is doloading[同名冲突]" })
	if _, err := d.MakeDir(context.Background(), root, "Other"); !errs.IsObjectAlreadyExists(err) {
		t.Fatalf("conflict: want ObjectAlreadyExists, got %v", err)
	}
}

func TestRecentChangesApply(t *testing.T) {
	file := func(fid, name string) *File { return &File{Fid: fid, FileName: name, File: true} }
	listing := []model.Obj{
		file("kept", "kept"),
		file("deleted", "deleted"),
		file("moved", "moved"),
		file("renamed", "old name"),
		file("caught-up", "new name"),
	}
	var r recentChanges
	r.remove("deleted")
	r.put("elsewhere", file("moved", "moved"))
	r.rename(file("renamed", "new name"))
	r.rename(file("caught-up", "new name"))
	r.put("dir", file("created", "created"))
	r.put("dir", file("video-only", "skipped"))
	r.put("dir", file("expired", "expired"))
	r.changes["expired"] = recentChange{parentID: "dir", file: file("expired", "expired"), expires: time.Now().Add(-time.Second)}

	got := r.apply("dir", listing, func(f *File) bool { return f.Fid != "video-only" })
	want := map[string]string{"kept": "kept", "renamed": "new name", "caught-up": "new name", "created": "created"}
	if len(got) != len(want) {
		t.Fatalf("apply returned %d objects %v, want %v", len(got), ids(got), want)
	}
	for _, obj := range got {
		if want[obj.GetID()] != obj.GetName() {
			t.Fatalf("apply returned %s named %q, want %v", obj.GetID(), obj.GetName(), want)
		}
	}
	if got[2] != listing[4] {
		t.Fatal("a listing that already shows the change must be used as is")
	}
}
