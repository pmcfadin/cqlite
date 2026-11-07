#!/usr/bin/env python3
"""#4451 census: EXTERNAL writer | early-exit reader, stage by stage.

usage: census.py [--sites] [--lists SUITE_LIST_FILE] [FILES...]
Counts every non-comment pipeline stage pair (W | R) where R can exit before EOF:
  head (any), grep -q/-m/--max-count/-l/-L/--quiet, sed with q/Q, awk with exit,
  `while read` loop whose body has break/return/exit, `read` (last stage = subshell reading one line).
Excluded: writers that are bash builtins (printf/echo/...: #4443's class) and lines
carrying `# sigpipe-fixture`.  `a | b | head` counts ONCE (pair b|head).
"""
import re, sys, collections

BUILTIN_W = {'printf', 'echo', 'declare', 'typeset', 'set', 'compgen', 'type', 'jobs',
             'export', 'alias', 'trap', 'pwd', 'dirs', 'true', ':', 'false', 'read',
             'local', 'readonly', 'umask', 'ulimit', 'times', 'help', 'hash'}
WRAP = {'command', 'builtin', 'exec', 'env', 'nice', 'timeout', 'stdbuf', 'sudo', 'time',
        'ionice', 'nohup', 'xargs_', 'gtimeout'}
LEAD = {'if', 'elif', 'then', 'else', 'do', '!', '{', '(', 'time', 'while', 'until'}
FILTERS = {'grep', 'egrep', 'fgrep', 'sed', 'awk', 'cut', 'tr', 'sort', 'uniq', 'tail', 'head',
           'wc', 'xargs', 'tee', 'rev', 'fold', 'nl', 'paste', 'column', 'jq', 'cat', 'tac', 'expand'}
REDUCERS = {'head', 'wc', 'uniq'}
SMALL_SRC = {'date', 'stat', 'wc', 'basename', 'dirname', 'readlink', 'realpath', 'uname', 'id',
             'whoami', 'hostname', 'seq', 'sha256sum', 'shasum', 'md5sum', 'cksum', 'nproc',
             'getconf', 'tty', 'lsb_release', 'which', 'command', 'git_small'}
SMALL_GIT = {'rev-parse', 'config', 'symbolic-ref', 'hash-object', 'describe', 'remote', 'show-ref',
             'merge-base', 'cat-file', 'var', 'version', 'check-ref-format', 'ls-remote', 'rev-list_count'}


def logical_lines(path):
    """yield (lineno, text, in_heredoc). Joins backslash and trailing-pipe continuations."""
    raw = open(path, encoding='utf-8', errors='replace').read().split('\n')
    i, n = 0, len(raw)
    hd = None
    while i < n:
        ln = raw[i]
        if hd:
            if (ln.strip() if hd[1] else ln) == hd[0]:
                hd = None
            else:
                yield (i + 1, ln, True)
            i += 1
            continue
        start = i
        text = ln
        while True:
            t = text.rstrip()
            if t.endswith('\\') and not t.endswith('\\\\') and i + 1 < n:
                i += 1
                text = t[:-1] + ' ' + raw[i].lstrip()
                continue
            if re.search(r'(?<![|])\|\s*$', t) and not t.lstrip().startswith('#') and i + 1 < n:
                i += 1
                text = t + ' ' + raw[i].lstrip()
                continue
            break
        probe = re.sub(r"'[^']*'|\"(?:\\.|[^\"\\])*\"", '""', text)
        probe = re.sub(r'\$\(\([^)]*\)\)', '', probe)
        m = re.search(r'(?<!<)<<(?![<(])(-?)\s*([\'"]?)([A-Za-z_][A-Za-z0-9_]*)\2', probe)
        if not m:
            # the delimiter itself may be quoted ("EOF"): look in the raw text
            m = re.search(r'(?<!<)<<(?![<(])(-?)\s*([\'"])([A-Za-z_][A-Za-z0-9_]*)\2', text)
        if m and not text.lstrip().startswith('#'):
            term, dash = m.group(3), bool(m.group(1))
            if any((r.strip() if dash else r) == term for r in raw[i + 1:]):
                hd = (term, dash)
        yield (start + 1, text, False)
        i += 1


def strip_comment(s):
    q = None
    i = 0
    out = []
    while i < len(s):
        c = s[i]
        if q:
            out.append(c)
            if c == '\\' and q == '"' and i + 1 < len(s):
                out.append(s[i + 1]); i += 2; continue
            if c == q:
                q = None
        else:
            if c == '\\' and i + 1 < len(s):
                out.append(c); out.append(s[i + 1]); i += 2; continue
            if c in '\'"':
                q = c
            elif c == '#' and (i == 0 or s[i - 1] in ' \t;({|&'):
                break
            out.append(c)
        i += 1
    return ''.join(out)


def match_close(s, i, open_c, close_c):
    """s[i] is open_c; return index of matching close_c, quote aware, or -1."""
    depth = 0
    q = None
    j = i
    while j < len(s):
        c = s[j]
        if q:
            if c == '\\' and q == '"':
                j += 2; continue
            if c == q:
                q = None
        else:
            if c == '\\':
                j += 2; continue
            if c in '\'"':
                q = c
            elif c == open_c:
                depth += 1
            elif c == close_c:
                depth -= 1
                if depth == 0:
                    return j
        j += 1
    return -1


def scan(text, out):
    """Return list of pipelines (each a list of stage strings) at this level; nested
    $(..) `..` <(..) (..) are scanned recursively into `out` and masked in the parent."""
    pipelines = []
    cur_stage = []
    stages = []
    i, n = 0, len(text)
    q = None

    def end_stage():
        nonlocal cur_stage
        stages.append(''.join(cur_stage).strip())
        cur_stage = []

    def end_pipe():
        nonlocal stages
        end_stage()
        if len(stages) >= 1:
            pipelines.append(stages)
        stages = []

    while i < n:
        c = text[i]
        two = text[i:i + 2]
        if q == "'":
            cur_stage.append(c)
            if c == "'":
                q = None
            i += 1; continue
        if c == '\\' and i + 1 < n:
            cur_stage.append(text[i:i + 2]); i += 2; continue
        if q == '"':
            if two == '$(' and text[i:i + 3] != '$((':
                j = match_close(text, i + 1, '(', ')')
                if j > 0:
                    out.extend(scan(text[i + 2:j], out)); cur_stage.append('$()'); i = j + 1; continue
            if c == '`':
                j = text.find('`', i + 1)
                if j > 0:
                    out.extend(scan(text[i + 1:j], out)); cur_stage.append('`` '); i = j + 1; continue
            cur_stage.append(c)
            if c == '"':
                q = None
            i += 1; continue
        # unquoted
        if c == "'":
            q = "'"; cur_stage.append(c); i += 1; continue
        if c == '"':
            q = '"'; cur_stage.append(c); i += 1; continue
        if text[i:i + 3] == '$((':
            j = text.find('))', i)
            j = j + 2 if j > 0 else n
            cur_stage.append('$((N))'); i = j; continue
        if two == '$(':
            j = match_close(text, i + 1, '(', ')')
            if j > 0:
                out.extend(scan(text[i + 2:j], out)); cur_stage.append('$()'); i = j + 1; continue
        if two in ('<(', '>(') and (i == 0 or text[i - 1] in ' \t'):
            j = match_close(text, i + 1, '(', ')')
            if j > 0:
                out.extend(scan(text[i + 2:j], out)); cur_stage.append('<()'); i = j + 1; continue
        if two == '${':
            j = match_close(text, i + 1, '{', '}')
            if j > 0:
                cur_stage.append(text[i:j + 1].replace('|', '\x00')); i = j + 1; continue
        if c == '`':
            j = text.find('`', i + 1)
            if j > 0:
                out.extend(scan(text[i + 1:j], out)); cur_stage.append('``'); i = j + 1; continue
        if two == '[[':
            j = text.find(']]', i)
            if j > 0:
                cur_stage.append('[[T]]'); i = j + 2; continue
        if two == '((':
            j = text.find('))', i)
            if j > 0:
                cur_stage.append('((N))'); i = j + 2; continue
        if c == '(' :
            j = match_close(text, i, '(', ')')
            if j > 0:
                out.extend(scan(text[i + 1:j], out)); cur_stage.append('( )'); i = j + 1; continue
        if c == ')':
            i += 1; continue
        if two == '||':
            end_pipe(); i += 2; continue
        if two == '&&':
            end_pipe(); i += 2; continue
        if two == '|&':
            end_stage(); i += 2; continue
        if c == '|':
            end_stage(); i += 1; continue
        if c in ';&' and not (c == '&' and i > 0 and text[i - 1] in '>&<') and not (c == '&' and two == '&>') :
            end_pipe(); i += 1; continue
        cur_stage.append(c)
        i += 1
    end_pipe()
    return pipelines


def words(stage):
    """crude word split honoring quotes; returns list of raw words (quotes kept)."""
    ws, cur, q = [], [], None
    i = 0
    while i < len(stage):
        c = stage[i]
        if q:
            cur.append(c)
            if c == '\\' and q == '"' and i + 1 < len(stage):
                cur.append(stage[i + 1]); i += 2; continue
            if c == q:
                q = None
        elif c == '\\' and i + 1 < len(stage):
            cur.append(stage[i:i + 2]); i += 2; continue
        elif c in '\'"':
            q = c; cur.append(c)
        elif c in ' \t':
            if cur:
                ws.append(''.join(cur)); cur = []
        else:
            cur.append(c)
        i += 1
    if cur:
        ws.append(''.join(cur))
    return ws


def unq(w):
    return re.sub(r'^["\']|["\']$', '', w)


def command_of(stage):
    """-> (cmd, args[list of raw words]) after stripping lead keywords/assignments/wrappers."""
    ws = words(stage)
    # drop leading case-label fragments or redirections-only
    k = 0
    while k < len(ws):
        w = ws[k]
        wu = unq(w)
        if wu in LEAD and wu not in ('while', 'until'):
            k += 1; continue
        if re.match(r'^[A-Za-z_][A-Za-z0-9_]*=', w) and not w.startswith('=='):
            k += 1; continue
        if wu in ('command', 'builtin', 'exec', 'nice', 'ionice', 'nohup', 'stdbuf', 'sudo', 'env'):
            k += 1
            while k < len(ws) and (ws[k].startswith('-') or re.match(r'^[A-Za-z_]\w*=', ws[k])):
                if wu == 'env' and unq(ws[k]) in ('-u', '-C', '-S', '--unset', '--chdir') and k + 1 < len(ws):
                    k += 1
                k += 1
            continue
        if wu in ('timeout', 'gtimeout'):
            k += 1
            while k < len(ws) and ws[k].startswith('-'):
                k += 1
            if k < len(ws) and re.match(r'^[0-9.]+[smhd]?$', unq(ws[k])):
                k += 1
            continue
        break
    if k >= len(ws):
        return ('', [])
    cmd = unq(ws[k])
    return (cmd, ws[k + 1:])


def base(c):
    return c.rsplit('/', 1)[-1]


def reader_kind(stage, full_text_following):
    """return reader category string or None."""
    cmd, args = command_of(stage)
    b = base(cmd)
    raw = stage
    if b == 'head':
        return 'head'
    if b in ('grep', 'egrep', 'fgrep'):
        for a in args:
            au = unq(a)
            if au in ('--quiet', '--silent'):
                return 'grep-q'
            if au.startswith('--max-count'):
                return 'grep-m'
            if au.startswith('--files-with') or au == '--files-without-match':
                return 'grep-l'
            if re.match(r'^-[A-Za-z0-9]*[qlL][A-Za-z0-9]*$', au) or re.match(r'^-[A-Za-z]*m[0-9]*$', au):
                if re.match(r'^-[A-Za-z0-9]*q', au):
                    return 'grep-q'
                if 'm' in au:
                    return 'grep-m'
                return 'grep-l'
            if re.match(r'^-m[0-9]+', au) or au == '-m':
                return 'grep-m'
        return None
    if b in ('sed', 'gsed'):
        scripts = re.findall(r"'([^']*)'|\"([^\"]*)\"", raw)
        text = ' '.join(a or b_ for a, b_ in scripts)
        text = re.sub(r's(.)(?:\\.|(?!\1).)*\1(?:\\.|(?!\1).)*\1[gpiIwe0-9]*', 's', text)
        text = re.sub(r'y(.)(?:\\.|(?!\1).)*\1(?:\\.|(?!\1).)*\1', 'y', text)
        text = re.sub(r'/(?:\\.|[^/])*/', '/', text)
        if re.search(r'(?:^|[;{}\s/\d$!,])[qQ]\d*\s*(?:[;}]|$)', text):
            return 'sed-q'
        return None
    if b in ('awk', 'gawk', 'mawk'):
        scripts = re.findall(r"'([^']*)'|\"([^\"]*)\"", raw)
        text = ' '.join(a or b_ for a, b_ in scripts)
        text = re.sub(r'"(?:\\.|[^"\\])*"', '""', text)
        text = re.sub(r'/(?:\\.|[^/\\])*/', '//', text)
        if re.search(r'\bexit\b', text):
            return 'awk-exit'
        return None
    if b == 'read':
        return 'read'
    ws = words(stage)
    cw = [unq(w) for w in ws]
    if cw[:1] and cw[0] in ('while', 'until'):
        body = full_text_following
        body = re.sub(r"'[^']*'|\"(?:\\.|[^\"\\])*\"", '""', body)
        if re.search(r'(?:^|[;&|{\s])(break|return|exit)\b', body):
            return 'while-break'
        return None
    return None



def positionals(cmdname, args):
    """non-option words; handles options that take a separate argument."""
    takes = {'grep': {'-e', '-f', '-m', '-A', '-B', '-C', '--max-count', '--include', '--exclude'},
             'sed': {'-e', '-f', '-i', '-s'}, 'awk': {'-v', '-F', '-f'}}.get(cmdname, set())
    pos, skip, have_e = [], False, False
    for a in args:
        au = unq(a)
        if skip:
            skip = False
            continue
        if au in ('<<<', '<', '>', '>>', '2>', '2>&1') or re.match(r'^[0-9]*[<>]', au):
            if au in ('<<<', '<', '>', '>>', '2>'):
                skip = True
            continue
        if au.startswith('-') and len(au) > 1 and not re.match(r'^-[0-9]+$', au):
            if au in takes:
                skip = True
                if au in ('-e', '-f'):
                    have_e = True
            continue
        pos.append(a)
    return pos, have_e


def has_file_operand(cmdname, args):
    pos, have_e = positionals(cmdname, args)
    need = 0 if have_e else 1
    if re.search(r'<<<|<\(', ' '.join(args)):
        return False
    return len(pos) > need

SRC_BIG_OPERAND = re.compile(r'GATE|agent-gate|_LOG|\$LOG|\.log\b|\blog\b|\$0\b|BASH_SOURCE|SCRIPT\b|\$SELF|corpus|DATASET|\.jsonl')


def writer_class(stage):
    cmd, args = command_of(stage)
    b = base(cmd)
    if not cmd:
        return ('empty', 'C-small', cmd)
    argtext = ' '.join(args)
    if b in BUILTIN_W:
        return ('builtin', 'C-small', cmd)
    if b in ('grep', 'egrep', 'fgrep', 'rg', 'sed', 'gsed', 'awk', 'gawk', 'mawk'):
        cls = 'grep' if b in ('grep', 'egrep', 'fgrep', 'rg') else ('sed' if 'sed' in b else 'awk')
        isfile = has_file_operand(cls if cls != 'grep' else 'grep', args)
        unrestricted = ((cls == 'grep' and any(re.match(r'^-[A-Za-z]*v', unq(a)) for a in args))
                        or (cls == 'sed' and not any(re.match(r'^-[A-Za-z]*n', unq(a)) for a in args)))
        if isfile and unrestricted and SRC_BIG_OPERAND.search(argtext):
            return (cls, 'A-direct', cmd)
        return (cls, 'B-file' if isfile else 'stdin-filter', cmd)
    if b == 'find':
        return ('find', 'A-direct', cmd)
    if b == 'git':
        sub = next((unq(a) for a in args if not unq(a).startswith('-') and not unq(a).startswith('$') and '=' not in unq(a)), '')
        if sub in SMALL_GIT:
            return ('git', 'C-small', cmd + ' ' + sub)
        return ('git', 'A-direct', cmd + ' ' + sub)
    if b == 'ls':
        return ('ls', 'A-direct' if re.search(r'/proc|-[A-Za-z]*R', argtext) else 'B-file', cmd)
    if b in ('cat', 'tac', 'zcat', 'xzcat', 'bzcat'):
        return ('cat', ('A-direct' if SRC_BIG_OPERAND.search(argtext) else 'B-file') if (args and not re.search(r'<<', argtext)) else 'stdin-filter', cmd)
    if b in ('bash', 'sh', 'zsh', 'dash') or b.endswith('.sh') or b.endswith('.bash') or cmd.startswith('./') \
       or re.search(r'GATE|agent-gate|SCRIPT|HOOK|\$\{?[A-Z_]+\}?$', cmd) or cmd.startswith('$') or cmd.startswith('"$'):
        return ('script-or-gate', 'A-direct', cmd)
    if b in ('python3', 'python', 'ruby', 'node', 'cargo', 'gh', 'curl', 'ps', 'ss', 'lsof', 'journalctl',
             'systemctl', 'dmesg', 'tar', 'unzip', 'du', 'df', 'mount', 'env', 'strace'):
        return ('other', 'A-direct' if b not in ('df', 'env') else 'B-file', cmd)
    if b in SMALL_SRC:
        return ('other', 'C-small', cmd)
    if b in FILTERS:
        return ('filter', 'stdin-filter', cmd)
    if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', cmd):
        return ('func-or-other', 'U-func', cmd)
    return ('other', 'U-func', cmd)


def analyze_file(path, want_sites):
    sites = []
    lines = list(logical_lines(path))
    for idx, (ln, text, hd) in enumerate(lines):
        if hd:
            continue
        s = text
        if 'sigpipe-fixture' in s:
            continue
        s = strip_comment(s)
        if not s.strip():
            continue
        # case labels:  a|b)  -> drop label
        s = re.sub(r'^\s*case\b.*?\bin\s+(?=\(?[^\s()]*\))', '', s)
        s = re.sub(r'^\s*\(?[^\s()"\'|]*(?:\|[^\s()"\'|]+)+\)\s*', '', s)
        out = []
        pipes = scan(s, out)
        for stages in pipes + out:
            if len(stages) < 2:
                continue
            for k in range(1, len(stages)):
                w, r = stages[k - 1], stages[k]
                # body lookahead for `while` readers
                following = r
                if re.match(r'^\s*(while|until)\b', r) and not re.search(r'\bdone\b', r):
                    depth = 1
                    buf = []
                    for (_, t2, h2) in lines[idx + 1: idx + 400]:
                        if h2:
                            continue
                        t2 = strip_comment(t2)
                        for tok in re.findall(r'\b(while|until|for|select|done)\b', t2):
                            depth += 1 if tok != 'done' else -1
                        buf.append(t2)
                        if depth <= 0:
                            break
                    following = r + ' ' + ' '.join(buf)
                rk = reader_kind(r, following)
                if not rk:
                    continue
                wc_, tier, cmd = writer_class(w)
                if wc_ in ('builtin', 'empty'):
                    EXCL[rk] += 1
                    continue
                # a stdin-reading filter inherits the chain source's volume tier
                if tier == 'stdin-filter':
                    src = stages[0]
                    swc, stier, scmd = writer_class(src)
                    bounded = False
                    for mid in stages[1:k]:
                        mc = base(command_of(mid)[0])
                        if mc in REDUCERS or reader_kind(mid, mid):
                            bounded = True
                    if k - 1 == 0:
                        tier = 'C-small'
                    elif swc == 'builtin':
                        tier = 'C-small'          # fed by printf/echo/here-string: a shell variable's bytes
                    elif bounded:
                        tier = 'C-small'
                    elif stier.startswith('A'):
                        tier = 'A-via-filter'
                    elif stier == 'U-func':
                        tier = 'U-func'
                    else:
                        tier = 'B-file'
                sites.append((path, ln, wc_, cmd, rk, tier, ' | '.join(stages)[:160]))
    return sites


EXCL = collections.Counter()


def main():
    args = sys.argv[1:]
    show = False
    files = []
    while args:
        a = args.pop(0)
        if a == '--sites':
            show = True
        elif a == '--lists':
            files += [l.strip() for l in open(args.pop(0)) if l.strip()]
        else:
            files.append(a)
    allsites = []
    for f in files:
        allsites += analyze_file(f, show)
    if show:
        for s in allsites:
            print('%s:%d: [%s/%s] %s | tier %s :: %s' % (s[0], s[1], s[2], s[3], s[4], s[5], s[6]))
        return
    print('TOTAL', len(allsites))
    print('excluded (builtin writer: #4443 class) by reader:', dict(EXCL), 'sum', sum(EXCL.values()))
    byf = collections.Counter(s[0] for s in allsites)
    print('FILES with sites', len(byf), 'of', len(files))
    print('-- per file (top 20)')
    for f, c in byf.most_common(20):
        print('%5d  %s' % (c, f))
    print('-- by writer')
    for k, c in collections.Counter(s[2] for s in allsites).most_common():
        print('%5d  %s' % (c, k))
    print('-- by reader')
    for k, c in collections.Counter(s[4] for s in allsites).most_common():
        print('%5d  %s' % (c, k))
    print('-- by tier')
    for k, c in sorted(collections.Counter(s[5] for s in allsites).items()):
        print('%5d  %s' % (c, k))
    print('-- tier x writer')
    tw = collections.Counter((s[5], s[2]) for s in allsites)
    for (t, w), c in sorted(tw.items()):
        print('%5d  %s %s' % (c, t, w))
    print('-- top "other"/func writers')
    for k, c in collections.Counter(s[3].split()[0] for s in allsites if s[2] in ('other', 'func-or-other', 'filter')).most_common(25):
        print('%5d  %s' % (c, k))


if __name__ == '__main__':
    main()
