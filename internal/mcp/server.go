package mcp

import (
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	mcpserver "github.com/mark3labs/mcp-go/server"
	"github.com/strata-spec/openstrata/internal/mcp/tools"
	"github.com/strata-spec/openstrata/internal/smif"
)

// Server is the MCP server runtime container.
type Server struct {
	semanticPath    string
	correctionsPath string
	dbPool          *pgxpool.Pool
	model           *smif.SemanticModel
	mu              sync.RWMutex
	mcpServer       *mcpserver.MCPServer
}

// New creates a new MCP server container.

func New(semanticPath, correctionsPath string, pool *pgxpool.Pool) (*Server, error) {
	model, err := Load(semanticPath, correctionsPath)
	if err != nil {
		return nil, fmt.Errorf("initialize mcp model: %w", err)
	}

	return &Server{
		semanticPath:    semanticPath,
		correctionsPath: correctionsPath,
		dbPool:          pool,
		model:           model,
	}, nil
}

// Start registers all tools and begins serving on stdio transport.
func (s *Server) Start(port int) error {
	_ = port
	s.mcpServer = mcpserver.NewMCPServer(
		"strata",
		"0.1.0",
		mcpserver.WithToolCapabilities(true),
	)

	listTool, listHandler := tools.ListModels(s.getModel)
	s.mcpServer.AddTool(listTool, listHandler)

	getTool, getHandler := tools.GetModel(s.getModel)
	s.mcpServer.AddTool(getTool, getHandler)

	searchTool, searchHandler := tools.SearchSemantic(s.getModel)
	s.mcpServer.AddTool(searchTool, searchHandler)

	runTool, runHandler := tools.RunSemanticSQL(s.getModel, s.dbPool)
	s.mcpServer.AddTool(runTool, runHandler)

	recordTool, recordHandler := tools.RecordCorrection(s, s.getModel)
	s.mcpServer.AddTool(recordTool, recordHandler)

	formatTool, formatHandler := tools.FormatSMIFContext(s.getModel)
	s.mcpServer.AddTool(formatTool, formatHandler)

	return mcpserver.ServeStdio(s.mcpServer)
}

// Reload re-reads semantic and corrections files and atomically swaps model.
func (s *Server) Reload() error {
	model, err := Load(s.semanticPath, s.correctionsPath)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.model = model
	return nil
}

func (s *Server) getModel() *smif.SemanticModel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.model
}

func (s *Server) ModelCount() int {
	m := s.getModel()
	if m == nil {
		return 0
	}
	return len(m.Models)
}

func (s *Server) CorrectionsPath() string {
	return s.correctionsPath
}

func (s *Server) SMIFVersion() string {
	m := s.getModel()
	if m == nil {
		return ""
	}
	return m.SMIFVersion
}

var _ tools.ReloadableServer = (*Server)(nil)
