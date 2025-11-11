#
# ~/.bashrc
#

# If not running interactively, don't do anything
[[ $- != *i* ]] && return

# Check if the .ntfyenv exists and if so, include it 
if [ -f $HOME/.ntfyenv ]; then
    . $HOME/.ntfyenv
fi

# Little cutie macros >w<

# Nice colors and show hidden files by default
alias ls='ls -a --color=auto'
alias sl='ls -a --color=auto'
alias grep='grep --color=auto'

# Use config command to manage my dotfiles on github from anywhere 
# Remind me to look into gnu stow
alias config='/usr/bin/git --git-dir=$HOME/.cfg/ --work-tree=$HOME'
alias conftui='gitui --directory $HOME/.cfg/  --workdir $HOME'

# List files currently being tracked by config
alias lsconfig='config ls-tree -r HEAD --name-only'

# Some git aliases
alias gs='git status'
alias ga='git add'
alias gp='git push'
alias gc='git commit'
alias gd='git diff'

# Im lazyyyyyy ^-^
alias hypr='nvim ~/.config/hypr/hyprland.conf'

# Make a directory and cd into it
alias mkcd='mkdir -p -- "$1" && cd -P -- "$1"'

# Add macros for sshing into my servers, both locally and remotely
alias shh="ssh 192.168.2.6 -p 43083"
alias trans='ssh fuck.wyfi.top -p 49357'
alias transs='ssh 192.168.2.69 -p 49357'
alias gender='ssh fuck.wyfi.top -p 28740'
alias genderr='ssh 192.168.2.15 -p 28740'

# Lol this is here but I never use it
alias n="nvim"

# How much of the history to store in ram in command count
HISTSIZE=10000
# How many commands to save on disk (im a maniac)
HISTFILESIZE=10000

# Set neovim as default editor
export EDITOR='nvim'
export VISUAL='nvim'

# Initialize some things I like in my terminal :3
eval "$(starship init bash)"
eval "$(fzf --bash)"
eval "$(zoxide init bash)"

# Finally, introduce yourself, with pokemon!
krabby random 1-6
