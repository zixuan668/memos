package v1

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	v1pb "github.com/usememos/memos/proto/gen/api/v1"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

const (
	maxListMemosLimit     = 200
	defaultListMemosLimit = 50
	MemoNamePrefix        = "memos/"
	ReactionNamePrefix    = "reactions/"
	defaultPageSize       = 50
	maxPageSize           = 100
)

func extractMemoUIDFromName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("memo name is required")
	}
	if !strings.HasPrefix(name, MemoNamePrefix) {
		return "", fmt.Errorf("invalid memo name: %s", name)
	}
	uid := strings.TrimPrefix(name, MemoNamePrefix)
	if uid == "" {
		return "", fmt.Errorf("invalid memo name: %s", name)
	}
	return uid, nil
}

func (s *APIV1Service) GetMemo(ctx context.Context, request *connect.Request[v1pb.GetMemoRequest]) (*connect.Response[v1pb.GetMemoResponse], error) {
	uid, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &uid})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if memo == nil {
		return nil, connect.NewError(connect.CodeNotFound, stderrors.New("memo not found"))
	}

	reactions, err := s.Store.ListReactions(ctx, &store.FindReaction{ContentID: &memo.UID})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	attachments, err := s.Store.ListAttachments(ctx, &store.FindAttachment{ResourceID: &memo.UID})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	relations, err := s.loadMemoRelations(ctx, memo)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	message, err := s.convertMemoFromStore(ctx, memo, reactions, attachments, relations)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.GetMemoResponse{
		Memo: message,
	}), nil
}

func (s *APIV1Service) ListMemos(ctx context.Context, request *connect.Request[v1pb.ListMemosRequest]) (*connect.Response[v1pb.ListMemosResponse], error) {
	pageSize := int(request.Msg.PageSize)
	if pageSize <= 0 {
		pageSize = defaultListMemosLimit
	}
	if pageSize > maxListMemosLimit {
		pageSize = maxListMemosLimit
	}

	find := &store.FindMemo{
		Limit: &pageSize,
	}

	if request.Msg.PageToken != "" {
		if parsed, err := parsePageToken(request.Msg.PageToken); err == nil {
			find.Offset = &parsed
		}
	}

	if request.Msg.Filter != "" {
		if request.Msg.Filter == "row_status == 'ARCHIVED'" {
			find.RowStatus = &store.Archived
		}
	}

	memos, err := s.Store.ListMemos(ctx, find)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	reactionsByUID := make(map[string][]*store.Reaction)
	attachmentsByUID := make(map[string][]*store.Attachment)

	memoUIDs := make([]string, 0, len(memos))
	for _, memo := range memos {
		memoUIDs = append(memoUIDs, memo.UID)
	}

	if len(memoUIDs) > 0 {
		reactions, err := s.Store.ListReactions(ctx, &store.FindReaction{
			ContentIDList: memoUIDs,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for _, reaction := range reactions {
			reactionsByUID[reaction.ContentID] = append(reactionsByUID[reaction.ContentID], reaction)
		}

		attachments, err := s.Store.ListAttachments(ctx, &store.FindAttachment{
			ResourceIDList: memoUIDs,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for _, attachment := range attachments {
			attachmentsByUID[attachment.ResourceID] = append(attachmentsByUID[attachment.ResourceID], attachment)
		}
	}

	relationMap, err := s.batchConvertMemoRelations(ctx, memos, false)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	responseMemos := make([]*v1pb.Memo, 0, len(memos))
	for _, memo := range memos {
		message, err := s.convertMemoFromStore(ctx, memo, reactionsByUID[memo.UID], attachmentsByUID[memo.UID], relationMap[memo.ID])
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		responseMemos = append(responseMemos, message)
	}

	var nextPageToken string
	if len(memos) == pageSize {
		offset := 0
		if find.Offset != nil {
			offset = *find.Offset
		}
		nextPageToken = buildPageToken(offset + len(memos))
	}

	return connect.NewResponse(&v1pb.ListMemosResponse{
		Memos:         responseMemos,
		NextPageToken: nextPageToken,
	}), nil
}

func (s *APIV1Service) CreateMemo(ctx context.Context, request *connect.Request[v1pb.CreateMemoRequest]) (*connect.Response[v1pb.CreateMemoResponse], error) {
	if request.Msg.Memo == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("memo is required"))
	}
	if strings.TrimSpace(request.Msg.Memo.Content) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("memo content is required"))
	}

	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, err)
	}

	memoUID := generateMemoUID()

	create := &store.Memo{
		UID:        memoUID,
		CreatorID:  user.ID,
		Content:    request.Msg.Memo.Content,
		Visibility: convertVisibilityToStore(request.Msg.Memo.Visibility),
		Payload:    &storepb.MemoPayload{},
	}

	if request.Msg.Memo.Pinned {
		create.Pinned = true
	}
	if request.Msg.Memo.Location != nil {
		create.Payload.Location = convertLocationToStore(request.Msg.Memo.Location)
	}
	if request.Msg.Memo.RemindTime != nil && request.Msg.Memo.RemindTime.IsValid() {
		create.Payload.RemindTime = request.Msg.Memo.RemindTime
	}
	if len(request.Msg.Memo.Tags) > 0 {
		create.Payload.Tags = request.Msg.Memo.Tags
	}
	if request.Msg.Memo.CreateTime != nil && request.Msg.Memo.CreateTime.IsValid() {
		create.CreatedTs = request.Msg.Memo.CreateTime.AsTime().Unix()
	}
	if request.Msg.Memo.UpdateTime != nil && request.Msg.Memo.UpdateTime.IsValid() {
		create.UpdatedTs = request.Msg.Memo.UpdateTime.AsTime().Unix()
	}

	memo, err := s.Store.CreateMemo(ctx, create)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(request.Msg.Memo.Attachments) > 0 {
		for _, attachment := range request.Msg.Memo.Attachments {
			if attachment == nil || attachment.Name == "" {
				continue
			}
			attachmentUID, err := extractAttachmentUIDFromName(attachment.Name)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
			patch := &store.UpdateAttachment{
				ResourceID: &memo.UID,
			}
			if _, err := s.Store.UpdateAttachment(ctx, attachmentUID, patch); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}
	}

	reactions, _ := s.Store.ListReactions(ctx, &store.FindReaction{ContentID: &memo.UID})
	attachments, _ := s.Store.ListAttachments(ctx, &store.FindAttachment{ResourceID: &memo.UID})
	relations, _ := s.loadMemoRelations(ctx, memo)

	message, err := s.convertMemoFromStore(ctx, memo, reactions, attachments, relations)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.CreateMemoResponse{
		Memo: message,
	}), nil
}

func (s *APIV1Service) UpdateMemo(ctx context.Context, request *connect.Request[v1pb.UpdateMemoRequest]) (*connect.Response[v1pb.UpdateMemoResponse], error) {
	if request.Msg.Memo == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("memo is required"))
	}
	if request.Msg.UpdateMask == nil || len(request.Msg.UpdateMask.Paths) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("update mask is required"))
	}

	uid, err := extractMemoUIDFromName(request.Msg.Memo.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &uid})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if memo == nil {
		return nil, connect.NewError(connect.CodeNotFound, stderrors.New("memo not found"))
	}

	update := &store.UpdateMemo{}

	for _, path := range request.Msg.UpdateMask.Paths {
		if path == "content" {
			update.Content = &request.Msg.Memo.Content
		} else if path == "visibility" {
			visibility := convertVisibilityToStore(request.Msg.Memo.Visibility)
			update.Visibility = &visibility
		} else if path == "pinned" {
			update.Pinned = &request.Msg.Memo.Pinned
		} else if path == "location" {
			payload := memo.Payload
			if payload == nil {
				payload = &storepb.MemoPayload{}
			}
			payload.Location = convertLocationToStore(request.Msg.Memo.Location)
			memo.Payload = payload
			update.Payload = payload
		} else if path == "remind_time" {
			payload := memo.Payload
			if payload == nil {
				payload = &storepb.MemoPayload{}
			}
			payload.RemindTime = request.Msg.Memo.RemindTime
			memo.Payload = payload
			update.Payload = payload
		} else if path == "attachments" {
			// Attachments are handled outside memo row update for now.
		} else if path == "relations" {
			// Relations are handled outside memo row update for now.
		} else if path == "create_time" {
			if request.Msg.Memo.CreateTime != nil && request.Msg.Memo.CreateTime.IsValid() {
				ts := request.Msg.Memo.CreateTime.AsTime().Unix()
				update.CreatedTs = &ts
			}
		} else if path == "update_time" {
			if request.Msg.Memo.UpdateTime != nil && request.Msg.Memo.UpdateTime.IsValid() {
				ts := request.Msg.Memo.UpdateTime.AsTime().Unix()
				update.UpdatedTs = &ts
			} else {
				now := time.Now().Unix()
				update.UpdatedTs = &now
			}
		}
	}

	updatedMemo, err := s.Store.UpdateMemo(ctx, uid, update)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	reactions, _ := s.Store.ListReactions(ctx, &store.FindReaction{ContentID: &updatedMemo.UID})
	attachments, _ := s.Store.ListAttachments(ctx, &store.FindAttachment{ResourceID: &updatedMemo.UID})
	relations, _ := s.loadMemoRelations(ctx, updatedMemo)

	message, err := s.convertMemoFromStore(ctx, updatedMemo, reactions, attachments, relations)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.UpdateMemoResponse{
		Memo: message,
	}), nil
}

func (s *APIV1Service) DeleteMemo(ctx context.Context, request *connect.Request[v1pb.DeleteMemoRequest]) (*connect.Response[v1pb.DeleteMemoResponse], error) {
	uid, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := s.Store.DeleteMemo(ctx, uid); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&v1pb.DeleteMemoResponse{}), nil
}

func (s *APIV1Service) UndeleteMemo(ctx context.Context, request *connect.Request[v1pb.UndeleteMemoRequest]) (*connect.Response[v1pb.UndeleteMemoResponse], error) {
	uid, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &uid})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if memo == nil {
		return nil, connect.NewError(connect.CodeNotFound, stderrors.New("memo not found"))
	}

	rowStatus := store.Normal
	updatedMemo, err := s.Store.UpdateMemo(ctx, uid, &store.UpdateMemo{
		RowStatus: &rowStatus,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	reactions, _ := s.Store.ListReactions(ctx, &store.FindReaction{ContentID: &updatedMemo.UID})
	attachments, _ := s.Store.ListAttachments(ctx, &store.FindAttachment{ResourceID: &updatedMemo.UID})
	relations, _ := s.loadMemoRelations(ctx, updatedMemo)

	message, err := s.convertMemoFromStore(ctx, updatedMemo, reactions, attachments, relations)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.UndeleteMemoResponse{
		Memo: message,
	}), nil
}

func (s *APIV1Service) CreateMemoComment(ctx context.Context, request *connect.Request[v1pb.CreateMemoCommentRequest]) (*connect.Response[v1pb.CreateMemoCommentResponse], error) {
	if request.Msg.Comment == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("comment is required"))
	}

	parentUID, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	parentMemo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &parentUID})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if parentMemo == nil {
		return nil, connect.NewError(connect.CodeNotFound, stderrors.New("parent memo not found"))
	}

	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, err)
	}

	commentUID := generateMemoUID()
	create := &store.Memo{
		UID:        commentUID,
		CreatorID:  user.ID,
		Content:    request.Msg.Comment.Content,
		Visibility: convertVisibilityToStore(request.Msg.Comment.Visibility),
		ParentUID:  &parentUID,
		Payload:    &storepb.MemoPayload{},
	}
	if request.Msg.Comment.Location != nil {
		create.Payload.Location = convertLocationToStore(request.Msg.Comment.Location)
	}
	if request.Msg.Comment.RemindTime != nil && request.Msg.Comment.RemindTime.IsValid() {
		create.Payload.RemindTime = request.Msg.Comment.RemindTime
	}

	comment, err := s.Store.CreateMemo(ctx, create)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	reactions, _ := s.Store.ListReactions(ctx, &store.FindReaction{ContentID: &comment.UID})
	attachments, _ := s.Store.ListAttachments(ctx, &store.FindAttachment{ResourceID: &comment.UID})
	relations, _ := s.loadMemoRelations(ctx, comment)

	message, err := s.convertMemoFromStore(ctx, comment, reactions, attachments, relations)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.CreateMemoCommentResponse{
		Memo: message,
	}), nil
}

func (s *APIV1Service) ListMemoComments(ctx context.Context, request *connect.Request[v1pb.ListMemoCommentsRequest]) (*connect.Response[v1pb.ListMemoCommentsResponse], error) {
	parentUID, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	find := &store.FindMemo{
		ParentUID: &parentUID,
	}
	memos, err := s.Store.ListMemos(ctx, find)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	reactionsByUID := make(map[string][]*store.Reaction)
	attachmentsByUID := make(map[string][]*store.Attachment)
	memoUIDs := make([]string, 0, len(memos))
	for _, memo := range memos {
		memoUIDs = append(memoUIDs, memo.UID)
	}

	if len(memoUIDs) > 0 {
		reactions, _ := s.Store.ListReactions(ctx, &store.FindReaction{ContentIDList: memoUIDs})
		for _, reaction := range reactions {
			reactionsByUID[reaction.ContentID] = append(reactionsByUID[reaction.ContentID], reaction)
		}
		attachments, _ := s.Store.ListAttachments(ctx, &store.FindAttachment{ResourceIDList: memoUIDs})
		for _, attachment := range attachments {
			attachmentsByUID[attachment.ResourceID] = append(attachmentsByUID[attachment.ResourceID], attachment)
		}
	}

	relationMap, err := s.batchConvertMemoRelations(ctx, memos, false)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	responseMemos := make([]*v1pb.Memo, 0, len(memos))
	for _, memo := range memos {
		message, err := s.convertMemoFromStore(ctx, memo, reactionsByUID[memo.UID], attachmentsByUID[memo.UID], relationMap[memo.ID])
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		responseMemos = append(responseMemos, message)
	}

	sort.Slice(responseMemos, func(i, j int) bool {
		return responseMemos[i].CreateTime.AsTime().Before(responseMemos[j].CreateTime.AsTime())
	})

	return connect.NewResponse(&v1pb.ListMemoCommentsResponse{
		Memos: responseMemos,
	}), nil
}

func (s *APIV1Service) UpsertMemoReaction(ctx context.Context, request *connect.Request[v1pb.UpsertMemoReactionRequest]) (*connect.Response[v1pb.UpsertMemoReactionResponse], error) {
	if request.Msg.Reaction == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("reaction is required"))
	}

	uid, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &uid})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if memo == nil {
		return nil, connect.NewError(connect.CodeNotFound, stderrors.New("memo not found"))
	}

	user, err := s.GetCurrentUser(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, err)
	}

	reaction, err := s.Store.UpsertReaction(ctx, &store.Reaction{
		CreatorID:    user.ID,
		ContentID:    memo.UID,
		ReactionType: request.Msg.Reaction.ReactionType,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	creatorMap, err := s.listUsersByID(ctx, []int32{reaction.CreatorID})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	reactionMessage, err := convertReactionFromStoreWithCreators(reaction, creatorMap)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.UpsertMemoReactionResponse{
		Reaction: reactionMessage,
	}), nil
}

func (s *APIV1Service) DeleteMemoReaction(ctx context.Context, request *connect.Request[v1pb.DeleteMemoReactionRequest]) (*connect.Response[v1pb.DeleteMemoReactionResponse], error) {
	name := request.Msg.Name
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("reaction name is required"))
	}

	parts := strings.Split(name, "/")
	if len(parts) < 3 {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("invalid reaction name"))
	}
	reactionUID := parts[len(parts)-1]
	if err := s.Store.DeleteReaction(ctx, reactionUID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&v1pb.DeleteMemoReactionResponse{}), nil
}

func (s *APIV1Service) SetMemoRelations(ctx context.Context, request *connect.Request[v1pb.SetMemoRelationsRequest]) (*connect.Response[v1pb.SetMemoRelationsResponse], error) {
	uid, err := extractMemoUIDFromName(request.Msg.Name)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	memo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &uid})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if memo == nil {
		return nil, connect.NewError(connect.CodeNotFound, stderrors.New("memo not found"))
	}

	// Remove all old relations
	oldRelations, err := s.Store.ListMemoRelations(ctx, &store.FindMemoRelation{MemoID: &memo.ID})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, relation := range oldRelations {
		if err := s.Store.DeleteMemoRelation(ctx, relation); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	// Add new relations
	for _, relation := range request.Msg.Relations {
		if relation == nil || relation.RelatedMemo == nil {
			continue
		}
		relatedUID, err := extractMemoUIDFromName(relation.RelatedMemo.Name)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		relatedMemo, err := s.Store.GetMemo(ctx, &store.FindMemo{UID: &relatedUID})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if relatedMemo == nil {
			return nil, connect.NewError(connect.CodeNotFound, stderrors.New("related memo not found"))
		}

		if err := s.Store.CreateMemoRelation(ctx, &store.MemoRelation{
			MemoID:        memo.ID,
			RelatedMemoID: relatedMemo.ID,
			Type:          convertMemoRelationTypeToStore(relation.Type),
		}); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	relations, err := s.loadMemoRelations(ctx, memo)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&v1pb.SetMemoRelationsResponse{
		Relations: relations,
	}), nil
}

func (s *APIV1Service) BatchUpdateMemos(ctx context.Context, request *connect.Request[v1pb.BatchUpdateMemosRequest]) (*connect.Response[v1pb.BatchUpdateMemosResponse], error) {
	if request.Msg.UpdateMask == nil || len(request.Msg.UpdateMask.Paths) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("update mask is required"))
	}
	if len(request.Msg.Names) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, stderrors.New("memo names are required"))
	}

	updatedMemos := make([]*v1pb.Memo, 0, len(request.Msg.Names))
	for _, name := range request.Msg.Names {
		resp, err := s.UpdateMemo(ctx, connect.NewRequest(&v1pb.UpdateMemoRequest{
			Memo: request.Msg.Memo,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: slices.Clone(request.Msg.UpdateMask.Paths),
			},
		}))
		if err != nil {
			slog.Warn("failed to batch update memo", slog.String("name", name), slog.Any("error", err))
			continue
		}
		if resp != nil && resp.Msg != nil && resp.Msg.Memo != nil {
			updatedMemos = append(updatedMemos, resp.Msg.Memo)
		}
	}

	return connect.NewResponse(&v1pb.BatchUpdateMemosResponse{
		Memos: updatedMemos,
	}), nil
}

func buildPageToken(offset int) string {
	return fmt.Sprintf("%d", offset)
}

func parsePageToken(token string) (int, error) {
	var offset int
	_, err := fmt.Sscanf(token, "%d", &offset)
	return offset, err
}

func generateMemoUID() string {
	return generateUID()
}
