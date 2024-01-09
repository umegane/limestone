# Limestone の WAL の取り扱い

* 2023-11-29 arakawa (NT)
  * r0 - 初稿
* 2023-12-21 arakawa (NT)
  * r1 - 永続化形式バージョン v1 上の誤りを修正、一部改稿

## この文書について

* Tsurugidb の永続化を担うコンポーネント limestone のうち、WAL の取り扱いについて定める
* WAL 以外の永続化に関する情報については本文書の範囲外とする

## 用語

* write version
  * 各エントリの論理的な順序を表したバージョン番号
* マニフェストファイル
  * 永続化データの形式等の情報を記載したファイル
* 永続化形式バージョン
  * 永続化データの形式を表すバージョン
* epoch ファイル
  * 永続化に成功した epoch の情報を記録したファイル
* durable epoch 番号
  * 永続化に成功した epoch の epoch 番号
* WAL ファイル
  * トランザクションのコミット内容を記録したファイル
* epoch 断片
  * WAL ファイルに含まれる、同一 epoch 内に書き込まれたトランザクションのコミット内容
* 確定 epoch 断片
  * epoch 断片のうち、永続化に成功したもの
* 未確定 epoch 断片
  * epoch 断片のうち、永続化の成功が確認できていないもの
* 完全 epoch 断片
  * epoch 断片のうち、形式が妥当である (well-formed) もの
* 不完全 epoch 断片
  * epoch 断片のうち、形式が妥当でない (ill-formed) もの
* snapshot
  * 特定時点のデータベースの状態を表したデータセット

## WAL のデータ形式

### ファイルの種類

WAL を構成するファイルには、以下の種類がある。

* [マニフェストファイル](#マニフェストファイルの形式)
* [WAL ファイル](#wal-ファイルの形式)
* [epoch ファイル](#epoch-ファイルの形式)

上記はいずれも WAL の格納領域 (`log_location` の設定値) の配下に配置される。
以降、当該パスを `${log_location}` と表記する。

### データ形式の表記

データ形式の表記は、C の構造体に似た以下のような形式を利用する。

```c
/* <structure-declaration> */
struct <name> {
    <field-declaration>*
};

/* <union-declaration> */
union <name> {
    <structure-prototype-declaration>+
};

/* <structure-prototype-declaration> */
struct <name> ;

/* <field-declaration> */
one of:
  <field-type> <name> <dimension>* ( = <value> )? ;
  EOF - end of file

/* <field-type> */
one of:
  u8 - unsigned 8bit integer
  u32 - unsigned 32bit integer (little endian)
  u64 - unsigned 64bit integer (little endian)
  ... - ellipsis

/* <dimension> */
one of:
  [] - 不明な繰り返し回数
  [<integer>] - 指定回数だけ繰り返し
  [<name>] - 同一構造体内のフィールドの値だけ繰り返し

/* <name> */
c-style identifier

/* value */
<integer>
```

上記のうち、共用体 (`union`) の表現は C のものと大きく異なる。
本文書における `union` とは、それに含まれる構造体のうち **いずれかの形式** をとるものとする。

### マニフェストファイルの形式

マニフェストファイルは永続化データの形式等の情報を記載したファイルで、 `${log_location}/limestone-manifest.json` に配置される。

マニフェストファイルは JSON 形式であり、以下のプロパティを有する。

プロパティ名 | 形式 | 設定値 | 概要
-------------|------|--------|-----
`format_version` | 十進数文字列 | `"1.0"` | マニフェストファイルの形式を表すバージョン (`major.minor`)
`persistent_format_version` | 整数 | `1` | 永続化データ形式バージョン

特に重要なのが `persistent_format_version` の値で、この値は [epoch ファイルの形式](#epoch-ファイルの形式) や [WAL ファイルの形式](#wal-ファイルの形式) などを定める永続化形式バージョンの情報である。
永続化形式バージョンが異なればそれぞれのファイルの形式も変わる可能性があるため、各ファイルを読み出すに先立ってこの情報を確認しなければならない。

なお、 `${log_location}` 配下にマニフェストファイル自体が存在しない場合、永続化形式バージョンは `0` であるとみなす。

以降の節では、表に記載された永続化データ形式バージョンにおける各ファイルのフォーマットについて紹介する。

### WAL ファイルの形式

Tsurugidb は Parallel Write-Ahead-Log (p-WAL) 方式を採用しており、トランザクションのコミットログを複数のファイルに分割して書き込んでいる。
それぞれのファイルを WAL ファイルとよび、 `${log_location}/pwal_<id>` に配置される。このうち、 `<id>` は上位を0埋めされた4桁の整数である。

個々の WAL ファイルは以下の `wal_file` 構造体に示すようなデータ構造をとる。

```c
struct wal_file {
    wal_file_header header;
    epoch_snippet snippets[];
    EOF
};

struct wal_file_header { ... };
struct epoch_snippet { ... };
```

フィールド名 | 概要
------------|------
`header` | [WAL ファイルヘッダー](#wal-ファイルヘッダーの形式)
`snippets` | [epoch 断片](#epoch-断片の形式) の繰り返し

#### WAL ファイルヘッダーの形式

WAL ファイルヘッダーは、WAL ファイルそのもののメタ情報を記載する領域である。

ただし、現在の永続化形式バージョンにおいて WAL ファイルヘッダーは空であり、以下の `wal_file_header` のような自明な構造をとる。

```c
struct wal_file_header {};
```

この領域には、将来以下のような情報を含めることを想定している。

* マジックナンバー
  * 当該ファイルが WAL ファイルであることを明確にする
* 当該 WAL ファイルの永続化データ形式バージョン
  * 複数の永続化データ形式バージョンを混在させるため
* 完全性情報
  * 永続化データとしてWALファイルがすべてそろっていることの確認
  * 間接的には、以下のような情報があるとよい
    * 自身のファイル名
    * 直前にローテートした WAL ファイルのファイル名
* 一意性情報
  * 同一のファイルが複数存在しないことの確認
    * 通し番号やUUIDなどをふればよい
* 作成日時
  * ファイル管理のため
* 暗号化に関する情報
  * WAL ファイルの内容を秘匿するための情報
* 圧縮方式に関する情報
  * epoch 断片部分を圧縮・展開するための情報
* 開始 epoch 番号
  * 再起動時に素早く当該 WAL ファイルの epoch 範囲を把握するため

#### epoch 断片の形式

epoch 断片は、各 epoch でコミットされたトランザクションのログを格納する領域である。
WAL ファイルには複数の epoch 断片が含まれ、それぞれは以下の `epoch_snippet` 構造体のような形式をとる。

```c
struct epoch_snippet {
    epoch_snippet_header header;
    wal_entry entries[];
    epoch_snippet_footer footer;
}

struct epoch_snippet_header { ... };
struct wal_entry { ... };
struct epoch_snippet_footer { ... };
```

フィールド名 | 概要
------------|------
`header` | [epoch 断片ヘッダー](#epoch-断片ヘッダーの形式)
`entries` | [WAL エントリ](#wal-エントリの形式) の一覧
`footer` | [epoch 断片フッター](#epoch-断片フッターの形式)

上記のうち、 `entries` の繰り返しは `footer` の出現を以って打ち切られる。
その判定は各構造体の先頭 1 オクテットで判断できるようになっている。

前述のとおり tsurugidb は p-WAL 方式を採用しているため、epoch でコミットされた内容は複数の epoch 断片に分けられて格納されることがほとんどである。
そのため、epoch の内容を復元するには、当該 epoch の内容が格納されたすべての WAL ファイルを確認する必要がある。

#### epoch 断片ヘッダーの形式

epoch 断片ヘッダーは、各 [epoch 断片](#epoch-断片の形式) のメタ情報を格納する領域である。
これは以下の `epoch_snippet_header` のような形式をとる。

```c
struct epoch_snippet_header {
    u8 entry_type = 2;
    u64 epoch_number;
};
```

フィールド名 | 概要
------------|------
`entry_type` | epoch 断片ヘッダーを表すオクテット (`2`)
`epoch_number` | 当該 epoch 断片が属する epoch 番号

#### WAL エントリの形式

WAL エントリは、当該 epoch 内でコミットが行われたトランザクションの、個々の書き込み内容を表す領域である。各エントリには単一の Key-Value エントリの内容が含まれる。
これは以下の `wal_entry` 共用体のような形式をとる。

```c
union wal_entry {
    struct wal_entry_put;
    struct wal_entry_remove;
};

struct wal_entry_put {
    u8 entry_type = 1;
    u32 key_length;
    u32 value_length;
    u64 storage_id;
    u8 key_data[key_length];
    u64 write_version_high;
    u64 write_version_low;
    u8 value_data[key_length];
};

struct wal_entry_remove {
    u8 entry_type = 5;
    u32 key_length;
    u64 storage_id;
    u8 key_data[key_length];
    u64 write_version_major;
    u64 write_version_minor;
};
```

構造体名 | 概要
--------|-----
`wal_entry_put` | Key-Value　の追加・更新を表すエントリ
`wal_entry_remove` | Key-Value の削除を表すエントリ

フィールド名 | 概要
------------|------
`entry_type` | エントリの種類を表すオクテット (追加・更新: `1`, 削除: `5`)
`key_length` | Key の長さ
`value_length` | Value の長さ
`storage_id` | 操作対象のストレージID
`key_data` | 対象の Key を表すオクテット列
`write_version_major` | 上位 write version
`write_version_minor` | 下位 write version
`value_data` | 対象の Value を表すオクテット列

#### epoch 断片フッターの形式

epoch 断片フッターは、各 [epoch 断片](#epoch-断片の形式) の末端を表す領域である。

ただし、現在の永続化形式バージョンにおいて WAL ファイルヘッダーは空であり、以下の `epoch_snippet_footer` のような自明な形式をとる。

これは以下の `epoch_snippet_footer` のような形式をとる。

```c
struct epoch_snippet_footer {};
```

この領域は、[WAL エントリ](#wal-エントリの形式) の繰り返しの終端を表す目的で必要である。
将来以下のような情報を含めることを想定している。

* 完全性情報
  * 当該 epoch 断片の内容が破損していないことを確認するための情報

### epoch ファイルの形式

epoch ファイルは永続化に成功した epoch 番号 (durable epoch 番号) を記録しておくためのファイルであり、 `${log_location}/epoch` に配置される。
このファイルは以下の `epoch_file` 構造体に示すようなデータ構造をとる。

```c
struct epoch_file {
    epoch_file_header header;
    durable_epoch_entry entries[];
    EOF
};

struct epoch_file_header { ... };
struct durable_epoch_entry { ... };
```

フィールド名 | 概要
------------|------
`header` | [epoch ファイルヘッダー](#epoch-ファイルヘッダーの形式)
`entries` | [永続化 epoch エントリ](#永続化-epoch-エントリの形式) の繰り返し

#### epoch ファイルヘッダーの形式

epoch ファイルヘッダーは、 epoch ファイルそのもののメタ情報を記載する領域である。

ただし、現在の永続化形式バージョンにおいて epoch ファイルヘッダーは空であり、以下の `epoch_file_header` のような自明な構造をとる。

```c
struct epoch_file_header {};
```

この領域には、将来以下のような情報を含めることを想定している。

* マジックナンバー
  * 当該ファイルが epoch ファイルであることを明確にする
* 作成日時
  * ファイル管理のため

#### 永続化 epoch エントリの形式

永続化 epoch エントリは、そこまでに永続化に成功した epoch (durable epoch) の情報を記録する領域である。

この領域は epoch ファイル内で繰り返し出現し、そのうち最後のエントリが最新の durable epoch を表している。

当該エントリは、以下の `durable_epoch_entry` のような形式をとる。

```c
struct durable_epoch_entry {
    u8 entry_type = 4;
    u64 epoch_number;
};
```

フィールド名 | 概要
------------|------
`entry_type` | 永続化 epoch エントリを表すオクテット (`4`)
`epoch_number` | 永続化に成功した epoch 番号 (durable epoch 番号)

この領域には、将来以下のような情報を含めることを想定している。

* 完全性情報
  * 当該エントリの内容が破損していないことを確認するための情報

## ファイルローテーション

以下のファイルは追記型のファイル構造をとっており、ファイルローテーションを行う場合がある。

* epoch ファイル
* WAL ファイル

ファイルローテーションを行う状況は、主に以下のいずれかである。

* ファイルサイズが既定値に達した場合
* オンラインバックアップ等、利用者の指示によりローテーションを行う場合

### WAL ファイルのローテーション

各 WAL ファイルはトランザクションのコミット内容を格納するものであり、ローテーションを行わないと際限なく大きくなっていってしまう。
このため、一定サイズを超過した WAL ファイルを別名にして保存し、続きの内容は新しい WAL ファイルを作成してそちらに格納する。

ローテートした WAL ファイルは `${log_location}/<source>.<unix-time>.<epoch>` に配置する。
このとき、それぞれのパラメータは以下のとおりである。

パラメータ名 | 形式 | 概要
------------|------|------
`source`    | 文字列 | ローテート対象の WAL ファイル名
`unix-time` | 左0埋め14桁整数 | ローテーションを行った時刻
`epoch`     | 整数 | 常に `0`

なお、ローテートされたファイルは以下のすべてを満たす場合に、ファイルシステムから削除してもよい。

* 当該ファイルの内容がすべて snapshot に反映され、かつ当該 snapshot が永続化された
* 当該ファイルは誰からも参照されていない
  * オンラインバックアップや、レプリケーションを行う際に古い WAL ファイルを参照する場合がある

ただし、WAL ファイルは以下の理由により、自動的に削除するのは推奨しない。

* 増分バックアップ
  * 前回バックアップからの増分を WAL に限定することで、ディスク容量を圧縮する関係で、古い WAL ファイルが必要
  * 古い WAL がない場合には増分検出が行えないため、完全バックアップが必要になる
* レプリケーション
  * 停止したレプリケーションを再開する際、古い WAL を送付して変更内容を通知する必要がある
  * 古い WAL がない場合、snapshot をレプリカに送付する必要がある
* 問題分析
  * 古い WAL を参照することで、過去のトランザクションの書き込み内容を把握できる

上記のため、ツールを作成し、明示的な指示があった場合のみ一定時間 (e.g. 30日) 経過したものを削除するなど行えることが望ましい。

----
TBD:

* WAL ローテーションの閾値を設定から行えるようにする
  * `log_rotation_threshold = n-MB`
* 以下のデータを付与するため、ローテートする対象のファイルにはフッタを付与するのはどうか
  * ファイルの作成時刻 (ローテートされた時刻)
    * ファイル削除時に一定以上古いかどうかを判断するため
  * ファイルに含まれる各 epoch 断片の epoch 番号または write version の範囲
    * snapshot 構築時に当該ファイルが対象であるか否かを判断するため
    * 削除対象にできるかどうかを素早く判断するため

### epoch ファイルのローテーション

各 epoch ファイルは durable epoch 番号を記録するためのものであるが、常に末尾にある最大値のみが有効である。
これを定期的にローテートすることで、当該ファイルの肥大化を防げる。

ローテートした WAL ファイルは `${log_location}/epoch.<unix-time>.<epoch>` に配置する。
このとき、それぞれのパラメータは以下のとおりである。

パラメータ名 | 形式 | 概要
------------|------|------
`source`    | 文字列 | ローテート対象の WAL ファイル名
`unix-time` | 左0埋め14桁整数 | ローテーションを行った時刻
`epoch`     | 整数 | ローテートを行った時点の durable epoch 番号

なお、ローテートされたファイルは、以下の条件をすべて満たす場合にファイルシステムから削除できる。

* ローテート後、新しく作成した epoch ファイルが空でない
* 当該ファイルは誰からも参照されていない
  * オンラインバックアップを行う際には、常にローテートされたファイルを参照する

上記を満たす場合、ローテートされた古い epoch ファイルはもはや不要であるため、当該ファイルを削除して問題ない。
古い WAL ファイルと異なり、 epoch ファイルは epoch の切り替わりを記録しているだけなので、古い情報に意味はない。

## データベースのリストア

Tsurugidb を終了後、再度実行した際には永続化されたデータベースの情報を読み出し、トランザクションエンジンを終了直前の状態に復元する必要がある。

この操作をデータベースのリストアとよぶが、これは主に以下の3つの工程に分けられる。

* [WAL リペア](#wal-リペア)
* [Snapshot リカバリ](#snapshot-リカバリ)
* [Index リストア](#index-リストア)

### WAL リペア

WAL リペアは、WAL を構成するファイルに不備があった際にそれらを修復し、正常な WAL として取り扱えるようにする操作である。

WAL リペアが必要となるケースは以下の2点のいずれかである。

* WAL ファイルに不完全 epoch 断片を含む
  * epoch 断片の書き込み中に tsurugidb が終了してしまった場合
* WAL ファイルに未確定 epoch 断片を含む
  * epoch 断片は書き込めたが、当該 epoch の group commit が完了する前に tsurugidb が終了してしまった場合
  * オンラインバックアップ等で、稼動中の tsurugidb の WAL を複製した場合

なお、WAL リペアは単独で行うだけでなく、後述の [Snapshot リカバリ](#snapshot-リカバリ) と同時に行うことができる。
それぞれの工程を単独で行う場合には各 WAL ファイルを2回走査する必要があるが、併せて行うことでその回数を削減できる。

#### 不完全 epoch 断片のリペア

対象の epoch 断片が **完全** epoch 断片であるには、以下をすべて満たす必要がある。

* 当該 epoch 断片に含まれる epoch 断片ヘッダーは well-formed である
* 当該 epoch 断片に含まれるすべての WAL エントリは well-formed である

上記のいずれかを満たさない場合、当該 epoch 断片は **不完全** epoch 断片として扱われる。

不完全 epoch 断片は、基本的に WAL の書き込み中に tsurugidb が(異常)終了してしまったケースのみを想定している
(それ以外で不完全なものが生まれると、WAL ファイル全体が信頼できない)。

このため、不完全 epoch 断片は以下のような特性を持つ:

* 当該 epoch 断片が含まれる WAL ファイルの、末尾の epoch 断片である
  * つまり、epoch 断片の途中に予期しないファイルの終端が出現する

不完全 epoch 断片のリペアは、次のように行うものとする。

* 当該 epoch 断片の先頭から WAL ファイルの末尾までを除去 (truncate) する

ただし、このプロセスは本質的に危険であるため、利用者に確認をとったうえで行うことが望ましい (必要であれば複製を作成してもらう)。

そのため、通常のリストアプロセスでは不完全な epoch 断片を見つけた時点でエラーを出力し、別途修復ツールを提供するか、特殊な起動オプションで tsurugidb を起動した際のみ行うものとする。

----
TBD:

* WAL ファイルの末尾以外に ill-formed な epoch 断片が出現するケースも考慮する
  * truncate するのではなく、不完全 epoch 断片として well-formed な形式に修復する
  * padding 等のエントリを用意し、 破損個所を読み飛ばすような仕組みを epoch 断片のフォーマットとして用意する
* 不完全 epoch 断片をリペアしたのちは、当該 WAL ファイルをローテートするのがよさそう
  * 大抵の場合、リペアする領域は WAL ファイルの末尾である
  * 追記する WAL ファイルの途中に破損データが含まれていると処理が面倒

#### 未確定 epoch 断片のリペア

対象の epoch 断片が **確定** epoch 断片であるには、以下をすべて満たす必要がある。

* 当該 epoch 断片は完全 epoch 断片である
* WAL 全体の durable epoch 番号を _D_ 、当該 epoch 断片の epoch 番号を _E_ としたとき、 _E &le; D_ が成り立つ
  * WAL 全体の durable epoch 番号は epoch ファイルから抽出できる
  * 各 epoch 断片の epoch 番号は epoch 断片ヘッダーから抽出できる

また、対象の epoch 断片が **未確定** epoch 断片であるには、以下をすべて満たす必要がある。

* 当該 epoch 断片は完全 epoch 断片である
* WAL 全体の durable epoch 番号を _D_ 、当該 epoch 断片の epoch 番号を _E_ としたとき、 _E &gt; D_ が成り立つ

未確定 epoch 断片のリペアは、次のように行うものとする。

* [epoch 断片ヘッダー](#epoch-断片ヘッダーの形式) を書き換え、当該 epoch 断片を「未確定 epoch 断片」を表すようにする

未確定 epoch 断片の epoch 断片ヘッダーは、次のようなデータ構造 `canceled_epoch_snippet_header` をとる。

```c
struct canceled_epoch_snippet_header {
    u8 entry_type = 6;
    u64 epoch_number;
};
```

フィールド名 | 概要
------------|------
`entry_type` | 未確定 epoch 断片の epoch 断片ヘッダーを表すオクテット (`6`)
`epoch_number` | 当該 epoch 断片が属する epoch 番号

後段の [Snapshot リカバリ](#snapshot-リカバリ) を行う際には、未確定 epoch 断片を読み飛ばすようにする。

### Snapshot リカバリ

Snapshot リカバリは、snapshot の内容と WAL の内容に差異があった際に、最新の WAL の内容を snapshot に反映させる操作を表す。
なお、 snapshot を常時構築していない構成では、tsurugidb の再起動時に必ず当該操作が必要となる。

WAL と snapshot は以下の点に差異がある:

* 提供可能なデータ
  * WAL - コミットされたすべてのトランザクションにおいて書き込まれたエントリの情報
  * snapshot - コミットされたすべてのトランザクションにおいて書き込まれたエントリのうち、論理的に最新 (write version が最大) のエントリ群
* データの整列形式
  * WAL - コミットされたトランザクションが epoch 順に並んでいる
  * snapshot - 各エントリはキー順に整列し、ランダムアクセス可能

Snapshot の構築は本文書の範囲外であるが、おおよその流れは以下のとおりである。

1. 現在の snapshot に未反映の確定 epoch 断片を各 WAL ファイルから抽出する (未確定 epoch 断片は読み飛ばす)
2. 当該 WAL ファイルに含まれるエントリを snapshot にマージする

ただし、実際には各エントリのうち、 write version が最大のもののみが利用可能になる。

### Index リストア

Index リストアは、 snapshot の内容を Tsurugidb のインデックスに反映させ、Tsurugidb 終了前の状態に復元する操作を表す。

なお、対象のインデックスは limestone 上には存在せず、limestone は当該インデックスを持つコンポーネントに snapshot を提供する役割のみを持つ。

インデックスの構築は本文書の対象外であるため、ここでは割愛する。
