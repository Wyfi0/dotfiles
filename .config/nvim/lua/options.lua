-- Global options!! ^w^
vim.opt.completeopt = {'menu', 'menuone', 'noselect'}
vim.opt.mouse = 'a'		-- Allow the mouse to be used in Nvim
vim.opt.clipboard = 'unnamedplus'

-- Tab Options
vim.opt.tabstop = 4		-- number of spaces when TAB
vim.opt.softtabstop = 4		-- number of spaces in tab when editing
vim.opt.shiftwidth = 4		-- insert 4 spaces per tab
vim.opt.expandtab = true	-- tabs do be spaces tho

-- Pretty ui
vim.opt.number = true		-- show absolute number
vim.opt.relativenumber = true	-- show relative number
vim.opt.splitbelow = true   -- when split, open one at the bottom
vim.opt.splitright = true   -- when split, open one at the left
vim.opt.termguicolors = true    -- enable 24-bit color in the tui
vim.opt.showmode = false    -- dont show active mode in the corner

-- Search config
vim.opt.incsearch = true    -- search incrementally while typing
vim.opt.hlsearch = true     -- highlight matches!
vim.opt.ignorecase = true   -- ignore case in search by default
vim.opt.smartcase = true    -- if upper case is specified, then use case!
