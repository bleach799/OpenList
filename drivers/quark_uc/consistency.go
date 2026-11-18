package quark

import (
	"context"
	"html"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

// Quark confirms uploads, renames, moves and deletes before /file/sort shows
// them, usually for a few seconds. OpenList caches whatever a listing returns,
// so a listing taken in that window hides a file that was just uploaded (WebDAV
// clients then get 404 and upload it again, which leaves duplicates) or keeps
// showing one that was just deleted. recentChanges remembers what this storage
// did itself for a while and lays it over fresh listings until Quark catches up.

const recentChangeTTL = time.Minute

type recentChange struct {
	parentID string // where the object is now, "" if not known
	file     *File  // nil once deleted
	expires  time.Time
}

type recentChanges struct {
	mu      sync.Mutex
	changes map[string]recentChange // by fid
}

// put records an object that now lives in parentID.
func (r *recentChanges) put(parentID string, f *File) {
	r.set(f.Fid, parentID, f, false)
}

// rename records a new name and keeps the parent if it is known.
func (r *recentChanges) rename(f *File) {
	r.set(f.Fid, "", f, true)
}

func (r *recentChanges) remove(fid string) {
	r.set(fid, "", nil, false)
}

func (r *recentChanges) set(fid, parentID string, f *File, keepParent bool) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune(now)
	if keepParent {
		parentID = r.changes[fid].parentID
	}
	if r.changes == nil {
		r.changes = make(map[string]recentChange)
	}
	r.changes[fid] = recentChange{parentID: parentID, file: f, expires: now.Add(recentChangeTTL)}
}

func (r *recentChanges) prune(now time.Time) {
	for fid, c := range r.changes {
		if now.After(c.expires) {
			delete(r.changes, fid)
		}
	}
}

// apply lays the recent changes over a fresh listing of parentID. A listed
// object is kept as soon as it shows the change, so the metadata reported by
// Quark takes over once it has caught up.
func (r *recentChanges) apply(parentID string, files []model.Obj, listable func(*File) bool) []model.Obj {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prune(time.Now())
	if len(r.changes) == 0 {
		return files
	}
	res := make([]model.Obj, 0, len(files))
	listed := make(map[string]struct{}, len(files))
	for _, obj := range files {
		listed[obj.GetID()] = struct{}{}
		c, ok := r.changes[obj.GetID()]
		switch {
		case !ok:
			res = append(res, obj)
		case c.file == nil:
			// deleted, the listing is behind
		case c.parentID != "" && c.parentID != parentID:
			// moved away, the listing is behind
		case c.file.GetName() != obj.GetName():
			// renamed, the listing is behind
			res = append(res, c.file)
		default:
			res = append(res, obj)
		}
	}
	for fid, c := range r.changes {
		if _, ok := listed[fid]; ok || c.file == nil || c.parentID != parentID || !listable(c.file) {
			continue
		}
		// created or moved here, the listing is behind
		res = append(res, c.file)
	}
	return res
}

// listable mirrors the filter applied by GetFiles.
func (d *QuarkOrUC) listable(f *File) bool {
	return !d.OnlyListVideoFile || f.IsDir() || f.Category == 1
}

// uploaded describes the file an upload has just created. Quark keeps the fid
// handed out by upload/pre, so op.Put can cache the real file right away
// instead of a temp entry.
func (d *QuarkOrUC) uploaded(dstDir model.Obj, stream model.FileStreamer, pre UpPreResp) model.Obj {
	if pre.Data.Fid == "" {
		return nil
	}
	now := time.Now().UnixMilli()
	f := &File{
		Fid:        pre.Data.Fid,
		FileName:   stream.GetName(),
		Size:       stream.GetSize(),
		File:       true,
		LCreatedAt: now,
		LUpdatedAt: now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	d.recent.put(dstDir.GetID(), f)
	return f
}

// waitForDir gives a directory that was just created a few seconds to show up
// in the listing of its parent.
func (d *QuarkOrUC) waitForDir(ctx context.Context, parentID, dirName string) *File {
	delay := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		dir, err := d.findDir(ctx, parentID, dirName)
		if err != nil {
			log.Debugf("[quark] failed to look for new dir %s: %v", dirName, err)
		}
		if dir != nil || attempt == 6 {
			return dir
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(delay*2, time.Second)
	}
}

func (d *QuarkOrUC) findDir(ctx context.Context, parentID, dirName string) (*File, error) {
	page, size := 1, 100
	query := map[string]string{
		"pdir_fid":             parentID,
		"_size":                strconv.Itoa(size),
		"_fetch_total":         "1",
		"fetch_all_file":       "1",
		"fetch_risk_file_name": "1",
		// folders first and newest first, so a new folder is normally on page one
		"_sort": "file_type:asc,updated_at:desc",
	}
	for {
		query["_page"] = strconv.Itoa(page)
		var resp SortResp
		_, err := d.request("/file/sort", http.MethodGet, func(req *resty.Request) {
			req.SetContext(ctx).SetQueryParams(query)
		}, &resp)
		if err != nil {
			return nil, err
		}
		for i := range resp.Data.List {
			f := &resp.Data.List[i]
			f.FileName = html.UnescapeString(f.FileName)
			if f.IsDir() && f.FileName == dirName {
				return f, nil
			}
		}
		if page*size >= resp.Metadata.Total {
			return nil, nil
		}
		page++
	}
}
