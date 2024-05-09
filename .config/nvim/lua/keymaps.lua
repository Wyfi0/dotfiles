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
-- Better window navigation, move between windows with control + h,j,k,l
vim.keymap.set('n', '<C-h>', '<C-w>h', opts)
vim.keymap.set('n', '<C-j>', '<C-w>j', opts)
vim.keymap.set('n', '<C-k>', '<C-w>k', opts)
vim.keymap.set('n', '<C-l>', '<C-w>l', opts)

