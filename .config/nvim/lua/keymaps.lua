-- keymaps!!!

-- common options
local opts = {
    noremap = true,     -- non-recursive?
    silent = true,      -- do not show message
}

-----------------
-- Normal mode --
-----------------

-- See ':h vim.map.set()'
-- Better window navigation, move between windows with control + arrows
vim.keymap.set('n', '<C-Up>', '<C-w>h', opts)
vim.keymap.set('n', '<C-Down>', '<C-w>j', opts)
vim.keymap.set('n', '<C-Left>', '<C-w>k', opts)
vim.keymap.set('n', '<C-Right>', '<C-w>l', opts)


-- Resize windows with control + h,j,k,l
vim.keymap.set('n', '<C-h>', ':resize -2<CR>', opts)
vim.keymap.set('n', '<C-j>', ':resize +2<CR>', opts)
vim.keymap.set('n', '<C-k>', ':vertical resize -2<CR>', opts)
vim.keymap.set('n', '<C-l>', ':vertical resize +2<CR>', opts)

-----------------
-- Visual mode --
-----------------

-- Hint: start visual mode with the same area as the previous area and the same mode
vim.keymap.set('v', '<', '<gv', opts)
vim.keymap.set('v', '>', '>gv', opts)

----------------------
-- Markdown Preview --
----------------------

vim.keymap.set('n', '<C-p>', ':MarkdownPreviewToggle', opts)

