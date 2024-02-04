#
# ~/.bashrc
#

# If not running interactively, don't do anything
[[ $- != *i* ]] && return

# Introduce yourself,
cutefetch

# Aliasssss sus [just maps a commant to another]
# Nice colors
alias ls='ls --color=auto'
alias grep='grep --color=auto'

# Use config command to manage my dotfiles on github from anywhere 
alias config='/usr/bin/git --git-dir=$HOME/.myconf/ --work-tree=$HOME'

# List files currently being tracked by config
alias lsconfig='config ls-tree -r HEAD --name-only'

# How much of the history to store in ram in command count
HISTSIZE=1000
# How many commands to save on disk (im a maniac)
HISTFILESIZE=10000

# Sets up the prompt before where you type
PS1='[\u@\h \W]\$ '

# Set neovim as default
export EDITOR='nvim'
export VISUAL='nvim'

# Aliasssss sus [just maps a command to another]
alias ls='ls --color=auto'
alias grep='grep --color=auto'
alias config='/usr/bin/git --git-dir=$HOME/.myconf/ --work-tree=$HOME'

# How much of the history to store in ram in command count
HISTSIZE=1000
# How many commands to save on disk
HISTFILESIZE=1000

# Sets up the prompt before where you type
PS1='[\u@\h \W]\$ '

# Set neovim as default
export EDITOR='nvim'
export VISUAL='nvim'

# Little cutie macros >w<
today() {
	echo -n "Today's date is: "
	date +"%A, %B %-d, %Y"
}
# Im lazyyyyyy ^-^
hypr() {
	nvim .config/hypr/hyprland.conf
}
# Make a directory and cd into it
mkcd() {
	mkdir -p -- "$1" && cd -P -- "$1"
}
# lol
shh() {
	ssh wyatt@192.168.0.12 -p 43083
}
# history awk
hawk() {
	history | awk "/$1/"
}
